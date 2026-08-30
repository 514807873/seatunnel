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

package org.apache.seatunnel.connectors.seatunnel.xjjdbc.sink;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.pangu.PanguStore;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.config.XjJdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.dialect.XjJdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.dialect.XjJdbcDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.util.Util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/** XjJdbc sink: full-load JDBC writer with field mapping, truncate and error landing. */
@Slf4j
public class XjJdbcSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private final CatalogTable catalogTable;
    private final SeaTunnelRowType seaTunnelRowType;
    private final XjJdbcSinkConfig sinkConfig;

    private JobContext jobContext;

    private volatile boolean preConfigDone = false;

    public XjJdbcSink(CatalogTable catalogTable, ReadonlyConfig config) {
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.sinkConfig = XjJdbcSinkConfig.of(config);
        if (StringUtils.isBlank(sinkConfig.getTable())) {
            sinkConfig.setTable(catalogTable.getTableId().getTableName());
        }
    }

    private synchronized void runPreConfig() {
        if (preConfigDone) {
            return;
        }
        XjJdbcDialect dialect = XjJdbcDialectFactory.getJdbcDialect(sinkConfig.getDbType());
        try (Connection conn = Util.getConnection(sinkConfig)) {
            conn.setAutoCommit(true);
            log.info("XjJdbc run pre config for table {}", sinkConfig.getTable());
            long deleteCount = sinkConfig.getPreConfig().doPreConfig(conn, dialect, sinkConfig);
            writeTruncateDeleteCount(deleteCount);
            preConfigDone = true;
        } catch (SQLException e) {
            throw new RuntimeException("XjJdbc pre config failed", e);
        }
    }

    private void writeTruncateDeleteCount(long deleteCount) {
        if (deleteCount <= 0) {
            return;
        }
        String flinkJobId = jobContext == null ? null : jobContext.getJobId();
        PanguStore.getInstance()
                .addHistoryRecord(
                        flinkJobId,
                        sinkConfig.getDbDatasourceId(),
                        sinkConfig.getDbSchema(),
                        sinkConfig.getTable(),
                        0L,
                        0L,
                        0L,
                        deleteCount,
                        0L,
                        0L);
    }

    @Override
    public String getPluginName() {
        return "XjJdbc";
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        runPreConfig();
        String flinkJobId = jobContext == null ? null : jobContext.getJobId();
        String panguJobId = jobContext == null ? null : jobContext.getPanguJobId();
        return new XjJdbcSinkWriter(seaTunnelRowType, context, sinkConfig, flinkJobId, panguJobId);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
