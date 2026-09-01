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

package org.apache.seatunnel.common.pangu;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Direct JDBC writer to Pangu MySQL / ClickHouse. Config comes from {@code seatunnel.yaml} {@code
 * engine.pangu-store}. Writes are queued on one daemon thread so checkpoint callbacks stay
 * non-blocking.
 */
@Slf4j
public final class PanguStore {

    private static final String OFFSET_SQL =
            "INSERT INTO seatunnel_jobs_offset (jobId, fileName, position) VALUES (?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE fileName = VALUES(fileName), position = VALUES(position)";

    private static final String STREAM_SQL =
            "INSERT INTO seatunnel_stream_record (jobId, rq, writeCount, insertCount, updateCount, deleteCount) "
                    + "VALUES (?, CURDATE(), ?, ?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE "
                    + "writeCount = IFNULL(writeCount, 0) + VALUES(writeCount), "
                    + "insertCount = IFNULL(insertCount, 0) + VALUES(insertCount), "
                    + "updateCount = IFNULL(updateCount, 0) + VALUES(updateCount), "
                    + "deleteCount = IFNULL(deleteCount, 0) + VALUES(deleteCount)";

    private static final String HISTORY_UPDATE_SQL =
            "UPDATE seatunnel_jobs_history SET jobStatus = 'FAILED', endTime = NOW(), "
                    + "duration = 0, exception = ? "
                    + "WHERE jobId = ? AND jobStatus IN ('WAITING', 'INITIALIZING') "
                    + "ORDER BY startTime DESC LIMIT 1";

    private static final String JOB_LOG_SQL =
            "INSERT INTO default.seatunnel_job_log "
                    + "(jobId, flinkJobId, threadName, createTime, loggerName, level, message) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?)";

    private static final String MONITOR_READ_SQL =
            "UPDATE seatunnel_jobs_monitor SET read_count = IFNULL(read_count, 0) + ?, "
                    + "endTime = NOW() WHERE job_id = ? ORDER BY startTime DESC LIMIT 1";

    /** history_record 只有自增 id，无业务唯一键。先按维度 UPDATE 一行，0 行再 INSERT。 */
    private static final String HISTORY_RECORD_UPDATE_SQL =
            "UPDATE seatunnel_jobs_history_record SET "
                    + "writeCount = IFNULL(writeCount, 0) + ?, "
                    + "insertCount = IFNULL(insertCount, 0) + ?, "
                    + "updateCount = IFNULL(updateCount, 0) + ?, "
                    + "deleteCount = IFNULL(deleteCount, 0) + ?, "
                    + "keepCount = IFNULL(keepCount, 0) + ?, "
                    + "errorCount = IFNULL(errorCount, 0) + ? "
                    + "WHERE flinkJobId = ? "
                    + "AND IFNULL(dataSourceId, '') = IFNULL(?, '') "
                    + "AND IFNULL(dbSchema, '') = IFNULL(?, '') "
                    + "AND IFNULL(tableName, '') = IFNULL(?, '') "
                    + "ORDER BY id DESC LIMIT 1";

    private static final String HISTORY_RECORD_INSERT_SQL =
            "INSERT INTO seatunnel_jobs_history_record "
                    + "(flinkJobId, dataSourceId, dbSchema, tableName, "
                    + "writeCount, insertCount, updateCount, deleteCount, keepCount, errorCount) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String LOGGER_ZETA_SUBMIT = "ZetaSubmit";

    private static final PanguStore INSTANCE = new PanguStore();

    private final String url;
    private final String username;
    private final String password;
    private final boolean enabled;
    private final String clickhouseUrl;
    private final String clickhouseUsername;
    private final String clickhousePassword;
    private final boolean clickhouseEnabled;
    private final ExecutorService executor;

    private PanguStore() {
        StoreConfig config = loadConfig();
        this.url = config.url;
        this.username = config.username;
        this.password = config.password;
        this.enabled = config.url != null && !config.url.isEmpty();
        this.clickhouseUrl = config.clickhouseUrl;
        this.clickhouseUsername = config.clickhouseUsername;
        this.clickhousePassword = config.clickhousePassword;
        this.clickhouseEnabled = config.clickhouseUrl != null && !config.clickhouseUrl.isEmpty();
        this.executor =
                Executors.newSingleThreadExecutor(
                        new ThreadFactory() {
                            @Override
                            public Thread newThread(Runnable r) {
                                Thread t = new Thread(r, "pangu-store");
                                t.setDaemon(true);
                                return t;
                            }
                        });
        if (enabled) {
            tryLoadMysqlDriver();
            log.info("PanguStore enabled, url={}", url);
        } else {
            log.info("PanguStore disabled: engine.pangu-store.url is empty");
        }
        if (clickhouseEnabled) {
            tryLoadClickHouseDriver();
            log.info("PanguStore ClickHouse enabled, url={}", clickhouseUrl);
        } else {
            log.info("PanguStore ClickHouse disabled: engine.pangu-store.clickhouse-url is empty");
        }
    }

    public static PanguStore getInstance() {
        return INSTANCE;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Master submit parse failed. Update the reserved history row (do not insert a second one) and
     * write the stack into ClickHouse job log when {@code pangu-job-id} is present.
     */
    public void recordSubmitFailure(String panguJobId, String engineJobId, Throwable error) {
        if (isBlank(panguJobId) || error == null) {
            return;
        }
        if (!enabled && !clickhouseEnabled) {
            return;
        }
        String flinkJobId = isBlank(engineJobId) ? "" : engineJobId;
        String message = error.getMessage();
        String stack = stackTrace(error);
        String threadName = Thread.currentThread().getName();
        Timestamp now = new Timestamp(System.currentTimeMillis());
        executor.execute(
                () -> {
                    if (enabled) {
                        updateFailedHistory(panguJobId, message);
                    }
                    if (clickhouseEnabled) {
                        insertJobLog(panguJobId, flinkJobId, threadName, now, stack);
                    }
                });
        log.warn(
                "PanguStore recorded submit failure, jobId={}, flinkJobId={}",
                panguJobId,
                flinkJobId);
    }

    public void upsertOffset(String panguJobId, String fileName, String position) {
        if (!enabled || isBlank(panguJobId) || isBlank(fileName)) {
            return;
        }
        executor.execute(
                () -> {
                    try (Connection conn = open();
                            PreparedStatement ps = conn.prepareStatement(OFFSET_SQL)) {
                        ps.setString(1, panguJobId);
                        ps.setString(2, fileName);
                        ps.setString(3, position);
                        ps.executeUpdate();
                    } catch (Exception e) {
                        log.warn(
                                "PanguStore upsert offset failed, jobId={}, file={}, pos={}",
                                panguJobId,
                                fileName,
                                position,
                                e);
                    }
                });
    }

    public void addJobMonitorRead(String panguJobId, long writeDelta) {
        if (!enabled || isBlank(panguJobId) || writeDelta <= 0) {
            return;
        }
        executor.execute(
                () -> {
                    try (Connection conn = open();
                            PreparedStatement ps = conn.prepareStatement(MONITOR_READ_SQL)) {
                        ps.setLong(1, writeDelta);
                        ps.setString(2, panguJobId);
                        ps.executeUpdate();
                    } catch (Exception e) {
                        log.warn(
                                "PanguStore add jobs_monitor read failed, jobId={}", panguJobId, e);
                    }
                });
    }

    public void addHistoryRecord(
            String flinkJobId,
            String dataSourceId,
            String dbSchema,
            String tableName,
            long writeCount,
            long insertCount,
            long updateCount,
            long deleteCount,
            long keepCount,
            long errorCount) {
        if (!enabled
                || isBlank(flinkJobId)
                || isBlank(tableName)
                || (writeCount == 0
                        && insertCount == 0
                        && updateCount == 0
                        && deleteCount == 0
                        && keepCount == 0
                        && errorCount == 0)) {
            return;
        }
        String ds = nullToEmpty(dataSourceId);
        String schema = nullToEmpty(dbSchema);
        executor.execute(
                () -> {
                    try (Connection conn = open()) {
                        int updated =
                                updateHistoryRecord(
                                        conn,
                                        flinkJobId,
                                        ds,
                                        schema,
                                        tableName,
                                        writeCount,
                                        insertCount,
                                        updateCount,
                                        deleteCount,
                                        keepCount,
                                        errorCount);
                        if (updated == 0) {
                            insertHistoryRecord(
                                    conn,
                                    flinkJobId,
                                    ds,
                                    schema,
                                    tableName,
                                    writeCount,
                                    insertCount,
                                    updateCount,
                                    deleteCount,
                                    keepCount,
                                    errorCount);
                        }
                    } catch (Exception e) {
                        log.warn(
                                "PanguStore add history_record failed, flinkJobId={}, table={}",
                                flinkJobId,
                                tableName,
                                e);
                    }
                });
    }

    public void addStreamRecord(
            String panguJobId,
            long writeCount,
            long insertCount,
            long updateCount,
            long deleteCount) {
        if (!enabled
                || isBlank(panguJobId)
                || (writeCount == 0 && insertCount == 0 && updateCount == 0 && deleteCount == 0)) {
            return;
        }
        executor.execute(
                () -> {
                    try (Connection conn = open();
                            PreparedStatement ps = conn.prepareStatement(STREAM_SQL)) {
                        ps.setString(1, panguJobId);
                        ps.setLong(2, writeCount);
                        ps.setLong(3, insertCount);
                        ps.setLong(4, updateCount);
                        ps.setLong(5, deleteCount);
                        ps.executeUpdate();
                    } catch (Exception e) {
                        log.warn("PanguStore add stream_record failed, jobId={}", panguJobId, e);
                    }
                });
    }

    private int updateHistoryRecord(
            Connection conn,
            String flinkJobId,
            String dataSourceId,
            String dbSchema,
            String tableName,
            long writeCount,
            long insertCount,
            long updateCount,
            long deleteCount,
            long keepCount,
            long errorCount)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(HISTORY_RECORD_UPDATE_SQL)) {
            ps.setLong(1, writeCount);
            ps.setLong(2, insertCount);
            ps.setLong(3, updateCount);
            ps.setLong(4, deleteCount);
            ps.setLong(5, keepCount);
            ps.setLong(6, errorCount);
            ps.setString(7, flinkJobId);
            ps.setString(8, dataSourceId);
            ps.setString(9, dbSchema);
            ps.setString(10, tableName);
            return ps.executeUpdate();
        }
    }

    private void insertHistoryRecord(
            Connection conn,
            String flinkJobId,
            String dataSourceId,
            String dbSchema,
            String tableName,
            long writeCount,
            long insertCount,
            long updateCount,
            long deleteCount,
            long keepCount,
            long errorCount)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(HISTORY_RECORD_INSERT_SQL)) {
            ps.setString(1, flinkJobId);
            ps.setString(2, dataSourceId);
            ps.setString(3, dbSchema);
            ps.setString(4, tableName);
            ps.setLong(5, writeCount);
            ps.setLong(6, insertCount);
            ps.setLong(7, updateCount);
            ps.setLong(8, deleteCount);
            ps.setLong(9, keepCount);
            ps.setLong(10, errorCount);
            ps.executeUpdate();
        }
    }

    private void updateFailedHistory(String panguJobId, String exception) {
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(HISTORY_UPDATE_SQL)) {
            ps.setString(1, exception);
            ps.setString(2, panguJobId);
            int updated = ps.executeUpdate();
            if (updated == 0) {
                log.warn(
                        "PanguStore submit failure found no WAITING/INITIALIZING history, jobId={}",
                        panguJobId);
            }
        } catch (Exception e) {
            log.warn("PanguStore update failed history failed, jobId={}", panguJobId, e);
        }
    }

    private void insertJobLog(
            String panguJobId,
            String flinkJobId,
            String threadName,
            Timestamp createTime,
            String message) {
        try (Connection conn = openClickHouse();
                PreparedStatement ps = conn.prepareStatement(JOB_LOG_SQL)) {
            ps.setString(1, panguJobId);
            ps.setString(2, flinkJobId);
            ps.setString(3, threadName == null ? "" : threadName);
            ps.setTimestamp(4, createTime);
            ps.setString(5, LOGGER_ZETA_SUBMIT);
            ps.setString(6, "ERROR");
            ps.setString(7, message == null ? "" : message);
            ps.executeUpdate();
        } catch (Exception e) {
            log.warn(
                    "PanguStore insert job_log failed, jobId={}, flinkJobId={}",
                    panguJobId,
                    flinkJobId,
                    e);
        }
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    private Connection openClickHouse() throws SQLException {
        return DriverManager.getConnection(clickhouseUrl, clickhouseUsername, clickhousePassword);
    }

    private static String stackTrace(Throwable error) {
        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isEmpty();
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static void tryLoadMysqlDriver() {
        String[] drivers = {"com.mysql.cj.jdbc.Driver", "com.mysql.jdbc.Driver"};
        for (String driver : drivers) {
            try {
                Class.forName(driver);
                return;
            } catch (ClassNotFoundException ignored) {
                // try next
            }
        }
        log.warn("MySQL JDBC driver not found on classpath, PanguStore writes may fail");
    }

    private static void tryLoadClickHouseDriver() {
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            log.warn(
                    "ClickHouse JDBC driver not found on classpath, PanguStore job_log writes may fail");
        }
    }

    private static StoreConfig loadConfig() {
        StoreConfig fallback = new StoreConfig("", "root", "", "", "default", "");
        File yaml = locateSeatunnelYaml();
        if (yaml == null || !yaml.isFile()) {
            log.info("PanguStore skip yaml, file not found");
            return fallback;
        }
        try {
            return parsePanguStoreBlock(yaml);
        } catch (Exception e) {
            log.warn("PanguStore failed to parse {}", yaml.getAbsolutePath(), e);
            return fallback;
        }
    }

    private static StoreConfig parsePanguStoreBlock(File yaml) throws Exception {
        String url = "";
        String username = "root";
        String password = "";
        String clickhouseUrl = "";
        String clickhouseUsername = "default";
        String clickhousePassword = "";
        boolean inStore = false;
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(new FileInputStream(yaml), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.startsWith("#") || trimmed.isEmpty()) {
                    continue;
                }
                if (!inStore) {
                    if (trimmed.startsWith("pangu-store:") || trimmed.equals("pangu-store")) {
                        inStore = true;
                    }
                    continue;
                }
                if (!line.startsWith(" ") && !line.startsWith("\t") && trimmed.endsWith(":")) {
                    break;
                }
                int colon = trimmed.indexOf(':');
                if (colon <= 0) {
                    continue;
                }
                String key = trimmed.substring(0, colon).trim();
                String value = unquote(resolveEnv(trimmed.substring(colon + 1).trim()));
                if ("url".equals(key)) {
                    url = value;
                } else if ("username".equals(key)) {
                    username = value.isEmpty() ? "root" : value;
                } else if ("password".equals(key)) {
                    password = value;
                } else if ("clickhouse-url".equals(key)) {
                    clickhouseUrl = value;
                } else if ("clickhouse-username".equals(key)) {
                    clickhouseUsername = value.isEmpty() ? "default" : value;
                } else if ("clickhouse-password".equals(key)) {
                    clickhousePassword = value;
                }
            }
        }
        return new StoreConfig(
                url, username, password, clickhouseUrl, clickhouseUsername, clickhousePassword);
    }

    private static String unquote(String value) {
        if (value.length() >= 2
                && ((value.startsWith("\"") && value.endsWith("\""))
                        || (value.startsWith("'") && value.endsWith("'")))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static String resolveEnv(String value) {
        if (!value.startsWith("${") || !value.endsWith("}")) {
            return value;
        }
        String key = value.substring(2, value.length() - 1);
        if (key.startsWith("?")) {
            key = key.substring(1);
        }
        String env = System.getenv(key);
        if (env != null) {
            return env;
        }
        String prop = System.getProperty(key);
        return prop == null ? "" : prop;
    }

    private static File locateSeatunnelYaml() {
        String home = System.getProperty("SEATUNNEL_HOME");
        if (isBlank(home)) {
            home = System.getProperty("seatunnel.home");
        }
        if (isBlank(home)) {
            home = System.getenv("SEATUNNEL_HOME");
        }
        if (isBlank(home)) {
            return null;
        }
        return new File(home, "config/seatunnel.yaml");
    }

    private static final class StoreConfig {
        private final String url;
        private final String username;
        private final String password;
        private final String clickhouseUrl;
        private final String clickhouseUsername;
        private final String clickhousePassword;

        private StoreConfig(
                String url,
                String username,
                String password,
                String clickhouseUrl,
                String clickhouseUsername,
                String clickhousePassword) {
            this.url = url == null ? "" : url.trim();
            this.username = username == null ? "root" : username;
            this.password = password == null ? "" : password;
            this.clickhouseUrl = clickhouseUrl == null ? "" : clickhouseUrl.trim();
            this.clickhouseUsername =
                    clickhouseUsername == null || clickhouseUsername.isEmpty()
                            ? "default"
                            : clickhouseUsername;
            this.clickhousePassword = clickhousePassword == null ? "" : clickhousePassword;
        }
    }
}
