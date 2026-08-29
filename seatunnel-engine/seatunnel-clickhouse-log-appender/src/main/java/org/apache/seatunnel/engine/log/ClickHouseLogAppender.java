/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.log;

import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import com.clickhouse.jdbc.ClickHouseDataSource;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * log4j2 custom appender: async batch write Zeta job logs into ClickHouse {@code
 * seatunnel_job_log}.
 *
 * <p>MDC {@code ST-JID} (engine Long) goes to {@code flinkJobId}; MDC {@code ST-PGID} (Pangu
 * interface id) goes to {@code jobId}. Only events carrying {@code ST-JID} are persisted.
 */
@Plugin(
        name = "ClickHouseLog",
        category = Core.CATEGORY_NAME,
        elementType = "appender",
        printObject = true)
public class ClickHouseLogAppender extends AbstractAppender {

    private static final String ST_JID = "ST-JID";
    private static final String ST_PGID = "ST-PGID";

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final int batchSize;
    private final long batchTimeoutMs;

    private final BlockingQueue<LogEvent> queue = new LinkedBlockingQueue<>(10000);
    private volatile boolean running = true;
    private ClickHouseDataSource dataSource;
    private Thread writerThread;

    protected ClickHouseLogAppender(
            String name,
            Filter filter,
            Layout<? extends Serializable> layout,
            boolean ignoreExceptions,
            String url,
            String username,
            String password,
            String table,
            int batchSize,
            long batchTimeoutMs) {
        super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
        this.url = url;
        this.username = username;
        this.password = password;
        this.table = table;
        this.batchSize = batchSize <= 0 ? 1 : batchSize;
        this.batchTimeoutMs = batchTimeoutMs <= 0 ? 100 : batchTimeoutMs;
    }

    @Override
    public void start() {
        try {
            Properties props = new Properties();
            props.setProperty("user", username == null ? "default" : username);
            if (password != null) {
                props.setProperty("password", password);
            }
            props.setProperty("socket_timeout", "30000");
            this.dataSource = new ClickHouseDataSource(url, props);

            this.writerThread = new Thread(this::processQueue, "ClickHouse-Log-Writer");
            this.writerThread.setDaemon(true);
            this.writerThread.start();
        } catch (Exception e) {
            error("Failed to initialize ClickHouse log appender: " + e.getMessage(), e);
        }
        super.start();
    }

    @Override
    public void append(LogEvent event) {
        if (!isStarted()) {
            return;
        }
        String flinkJobId = event.getContextData().getValue(ST_JID);
        if (flinkJobId == null || flinkJobId.isEmpty()) {
            return;
        }
        if (!queue.offer(event.toImmutable())) {
            error("ClickHouse log queue is full, discarding log: " + event.getMessage());
        }
    }

    private void processQueue() {
        String sql =
                "INSERT INTO "
                        + table
                        + " (jobId, flinkJobId, threadName, createTime, loggerName, level, message)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?)";
        List<LogEvent> batch = new ArrayList<>(batchSize);
        while (running || !queue.isEmpty()) {
            try {
                batch.clear();
                LogEvent first = queue.poll(batchTimeoutMs, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                queue.drainTo(batch, batchSize - 1);

                try (Connection conn = dataSource.getConnection();
                        PreparedStatement stmt = conn.prepareStatement(sql)) {
                    for (LogEvent event : batch) {
                        String flinkJobId = event.getContextData().getValue(ST_JID);
                        String panguJobId = event.getContextData().getValue(ST_PGID);
                        stmt.setString(1, panguJobId == null ? "" : panguJobId);
                        stmt.setString(2, flinkJobId == null ? "" : flinkJobId);
                        stmt.setString(3, event.getThreadName());
                        stmt.setTimestamp(4, new java.sql.Timestamp(event.getTimeMillis()));
                        stmt.setString(5, event.getLoggerName());
                        stmt.setString(6, event.getLevel().toString());
                        stmt.setString(
                                7,
                                event.getMessage() == null
                                        ? ""
                                        : event.getMessage().getFormattedMessage());
                        stmt.addBatch();
                    }
                    stmt.executeBatch();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                error("Failed to insert logs to ClickHouse: " + e.getMessage(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        running = false;
        if (writerThread != null) {
            writerThread.interrupt();
            try {
                writerThread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return super.stop(timeout, timeUnit);
    }

    @PluginFactory
    public static ClickHouseLogAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("url") String url,
            @PluginAttribute("username") String username,
            @PluginAttribute("password") String password,
            @PluginAttribute(value = "table", defaultString = "default.seatunnel_job_log")
                    String table,
            @PluginAttribute(value = "batchSize", defaultInt = 1) int batchSize,
            @PluginAttribute(value = "batchTimeoutMs", defaultLong = 100) long batchTimeoutMs,
            @PluginElement("Filter") Filter filter,
            @PluginElement("Layout") Layout<? extends Serializable> layout) {
        if (name == null) {
            LOGGER.error("No name provided for ClickHouseLogAppender");
            return null;
        }
        return new ClickHouseLogAppender(
                name,
                filter,
                layout,
                true,
                url,
                username,
                password,
                table,
                batchSize,
                batchTimeoutMs);
    }
}
