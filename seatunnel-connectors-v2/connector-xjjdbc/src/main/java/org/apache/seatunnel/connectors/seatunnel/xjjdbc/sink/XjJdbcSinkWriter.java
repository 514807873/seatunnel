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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.state.CheckpointListener;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.pangu.PanguJobIds;
import org.apache.seatunnel.common.pangu.PanguStore;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.pangu.PanguStreamCounter;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.config.XjJdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.converter.ColumnMapper;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.dialect.XjJdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.dialect.XjJdbcDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.util.Util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Full-load (complete) sink writer: buffered batch INSERT with per-row retry and error landing. */
@Slf4j
public class XjJdbcSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements CheckpointListener {

    private final SeaTunnelRowType sourceRowType;
    private final XjJdbcSinkConfig config;
    private final XjJdbcDialect dialect;
    private final String flinkJobId;
    private final String panguJobId;
    private final int subtaskIndex;
    private final PanguStreamCounter streamCounter = new PanguStreamCounter();

    private final Connection conn;
    private final List<ColumnMapper> columnMappers;
    private final String insertSql;

    private final List<SeaTunnelRow> buffer = new ArrayList<>();
    private final MidCount midCount = new MidCount();
    private final Set<String> sqlErrorType = new HashSet<>();

    private Connection errorConn;
    private boolean truncated = false;
    private long lastMonitorInsertCount = 0L;
    private long lastHistoryWriteCount = 0L;
    private long lastHistoryInsertCount = 0L;
    private long lastHistoryErrorCount = 0L;

    public XjJdbcSinkWriter(
            SeaTunnelRowType sourceRowType,
            SinkWriter.Context context,
            XjJdbcSinkConfig config,
            String flinkJobId,
            String panguJobId) {
        this.sourceRowType = sourceRowType;
        this.config = config;
        this.flinkJobId = flinkJobId;
        this.panguJobId = PanguJobIds.resolve(panguJobId);
        this.subtaskIndex = context.getIndexOfSubtask();
        this.dialect = XjJdbcDialectFactory.getJdbcDialect(config.getDbType());
        try {
            this.conn = Util.getConnection(config);
            this.conn.setAutoCommit(dialect.useAutoCommit());
            this.dialect.initConnection(this.conn);
            this.columnMappers = initColumnMappers();
            this.insertSql = dialect.insertSql(config, columnMappers);
            log.info("XjJdbc sink insert sql: {}", insertSql);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to open XjJdbc sink writer", e);
        }
    }

    private List<ColumnMapper> initColumnMappers() throws SQLException {
        Map<String, String> fieldMapper = config.getFieldMapper();
        if (fieldMapper == null || fieldMapper.isEmpty()) {
            fieldMapper = new LinkedHashMap<>();
            for (String fieldName : sourceRowType.getFieldNames()) {
                fieldMapper.put(fieldName, fieldName);
            }
        }
        List<String> sinkColumns = new ArrayList<>(fieldMapper.values());
        Map<String, String> sinkDbTypes = dialect.sinkColumnDbTypes(conn, config, sinkColumns);
        List<ColumnMapper> mappers = new ArrayList<>();
        for (Map.Entry<String, String> entry : fieldMapper.entrySet()) {
            String sourceColumn = entry.getKey();
            String sinkColumn = entry.getValue();
            int pos = indexOfField(sourceColumn);
            if (pos < 0) {
                throw new IllegalArgumentException(
                        "field_mapper source field not found in upstream: " + sourceColumn);
            }
            ColumnMapper mapper = new ColumnMapper();
            mapper.setSourceColumnName(sourceColumn);
            mapper.setSourceRowPosition(pos);
            mapper.setSourceSqlType(sourceRowType.getFieldType(pos).getSqlType());
            mapper.setSinkColumnName(sinkColumn);
            mapper.setSinkColumnDbType(sinkDbTypes.get(sinkColumn));
            mappers.add(mapper);
        }
        return mappers;
    }

    private int indexOfField(String name) {
        int idx = sourceRowType.indexOf(name);
        if (idx >= 0) {
            return idx;
        }
        String[] fieldNames = sourceRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (element == null || RowKind.UPDATE_BEFORE.equals(element.getRowKind())) {
            return;
        }
        midCount.setWriteCount(midCount.getWriteCount() + 1);
        streamCounter.accept(element);
        truncateOnFirstRowIfNeeded();
        buffer.add(element);
        if (buffer.size() >= config.getBatchSize()) {
            flush();
        }
    }

    private void truncateOnFirstRowIfNeeded() throws IOException {
        if (truncated || subtaskIndex != 0 || !config.getPreConfig().isTruncateOnFirstRow()) {
            return;
        }
        String truncateSql = dialect.truncateTable(config);
        try {
            long deleteCount = dialect.countTableRows(conn, config);
            log.info(
                    "XjJdbc first-row truncate target table: {}, deleteCount={}",
                    truncateSql,
                    deleteCount);
            try (Statement st = conn.createStatement()) {
                st.execute(truncateSql);
                if (!dialect.useAutoCommit()) {
                    conn.commit();
                }
            }
            truncated = true;
            writeTruncateDeleteCount(deleteCount);
        } catch (SQLException e) {
            throw new IOException("Failed to truncate target table: " + truncateSql, e);
        }
    }

    private void writeTruncateDeleteCount(long deleteCount) {
        if (deleteCount <= 0) {
            return;
        }
        PanguStore.getInstance()
                .addHistoryRecord(
                        flinkJobId,
                        config.getDbDatasourceId(),
                        config.getDbSchema(),
                        config.getTable(),
                        0L,
                        0L,
                        0L,
                        deleteCount,
                        0L,
                        0L);
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (SeaTunnelRow row : buffer) {
                bindRow(ps, row);
                ps.addBatch();
            }
            ps.executeBatch();
            if (!dialect.useAutoCommit()) {
                conn.commit();
            }
            midCount.setInsertCount(midCount.getInsertCount() + buffer.size());
        } catch (SQLException e) {
            log.error("XjJdbc batch insert failed, fall back to one-by-one. sql={}", insertSql, e);
            rollbackQuietly();
            flushOneByOne();
        } finally {
            buffer.clear();
        }
        flushProgress();
    }

    private void flushProgress() {
        streamCounter.flush(panguJobId);
        flushJobMonitorRead();
        flushHistoryRecord();
    }

    private void flushOneByOne() {
        for (SeaTunnelRow row : buffer) {
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                bindRow(ps, row);
                ps.executeUpdate();
                if (!dialect.useAutoCommit()) {
                    conn.commit();
                }
                midCount.setInsertCount(midCount.getInsertCount() + 1);
            } catch (SQLException e) {
                rollbackQuietly();
                midCount.setErrorCount(midCount.getErrorCount() + 1);
                recordError(row, e);
            }
        }
    }

    private void bindRow(PreparedStatement ps, SeaTunnelRow row) throws SQLException {
        for (int i = 0; i < columnMappers.size(); i++) {
            ColumnMapper mapper = columnMappers.get(i);
            Object value = row.getField(mapper.getSourceRowPosition());
            dialect.bindValue(
                    ps, i + 1, value, mapper.getSourceSqlType(), mapper.getSinkColumnDbType());
        }
    }

    private void rollbackQuietly() {
        if (dialect.useAutoCommit()) {
            return;
        }
        try {
            conn.rollback();
        } catch (SQLException e) {
            log.warn("XjJdbc rollback failed", e);
        }
    }

    private void recordError(SeaTunnelRow row, SQLException e) {
        if (!config.isRecordErrorData()) {
            return;
        }
        String message = e.getMessage();
        if (midCount.getErrorCount() > config.getMaxErrorNumber()
                || sqlErrorType.contains(message)) {
            return;
        }
        sqlErrorType.add(message);

        Map<String, String> data = new LinkedHashMap<>();
        for (ColumnMapper mapper : columnMappers) {
            data.put(
                    mapper.getSourceColumnName(),
                    Util.object2String(row.getField(mapper.getSourceRowPosition())));
        }
        String errorData = JsonUtils.toJsonString(data);
        log.info("XjJdbc error row: {}, message: {}", errorData, message);

        if (StringUtils.isBlank(config.getErrorRecordUrl())) {
            return;
        }
        try {
            if (errorConn == null || errorConn.isClosed()) {
                errorConn =
                        Util.getConnection(
                                config.getErrorRecordUrl(),
                                config.getErrorRecordDriver(),
                                config.getErrorRecordUser(),
                                config.getErrorRecordPassword());
                errorConn.setAutoCommit(true);
            }
            String sql =
                    "insert into "
                            + config.getErrorRecordTable()
                            + " (flinkJobId,dataSourceId,dbSchema,tableName,errorData,errorMessage)"
                            + " values (?,?,?,?,?,?)";
            try (PreparedStatement ps = errorConn.prepareStatement(sql)) {
                ps.setString(1, flinkJobId);
                ps.setString(2, config.getDbDatasourceId());
                ps.setString(3, config.getDbSchema());
                ps.setString(4, config.getTable());
                ps.setString(5, errorData);
                ps.setString(6, message);
                ps.executeUpdate();
            }
        } catch (Exception ex) {
            log.warn("XjJdbc failed to persist error record", ex);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        flushProgress();
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
            flushProgress();
            log.info(
                    "XjJdbc sink subtask {} finished. write={}, insert={}, error={}",
                    subtaskIndex,
                    midCount.getWriteCount(),
                    midCount.getInsertCount(),
                    midCount.getErrorCount());
        } finally {
            closeQuietly(conn);
            closeQuietly(errorConn);
        }
    }

    private void flushJobMonitorRead() {
        long current = midCount.getInsertCount();
        long delta = current - lastMonitorInsertCount;
        if (delta <= 0) {
            return;
        }
        lastMonitorInsertCount = current;
        PanguStore.getInstance().addJobMonitorRead(panguJobId, delta);
    }

    private void flushHistoryRecord() {
        long writeDelta = midCount.getWriteCount() - lastHistoryWriteCount;
        long insertDelta = midCount.getInsertCount() - lastHistoryInsertCount;
        long errorDelta = midCount.getErrorCount() - lastHistoryErrorCount;
        if (writeDelta <= 0 && insertDelta <= 0 && errorDelta <= 0) {
            return;
        }
        lastHistoryWriteCount = midCount.getWriteCount();
        lastHistoryInsertCount = midCount.getInsertCount();
        lastHistoryErrorCount = midCount.getErrorCount();
        PanguStore.getInstance()
                .addHistoryRecord(
                        flinkJobId,
                        config.getDbDatasourceId(),
                        config.getDbSchema(),
                        config.getTable(),
                        writeDelta,
                        insertDelta,
                        0L,
                        0L,
                        0L,
                        errorDelta);
    }

    private void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("XjJdbc failed to close connection", e);
            }
        }
    }
}
