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

package org.apache.seatunnel.connectors.seatunnel.xjjdbc.dialect;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.config.XjJdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.converter.ColumnMapper;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.util.Util;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * SQL/binding strategy of a particular database. Stateless and immutable.
 *
 * <p>The connector only needs the full-load INSERT path, so this interface keeps just the methods
 * for identifier quoting, table cleaning, INSERT generation, reading the sink column DB types and
 * type-aware value binding.
 */
public interface XjJdbcDialect extends Serializable {

    String dialectName();

    /** Quote a table/column identifier. The default keeps the identifier untouched. */
    default String quoteIdentifier(String identifier) {
        return identifier;
    }

    /** Some databases (ClickHouse, Trino) do not use JDBC transactions. */
    default boolean useAutoCommit() {
        return false;
    }

    /** Hook to initialize a freshly opened connection (e.g. set session for Trino). */
    default void initConnection(Connection connection) throws SQLException {
        // no-op by default
    }

    default String tableWithSchema(XjJdbcSinkConfig config) {
        if (StringUtils.isNotBlank(config.getDbSchema())) {
            return quoteIdentifier(config.getDbSchema()) + "." + quoteIdentifier(config.getTable());
        }
        return quoteIdentifier(config.getTable());
    }

    default String truncateTable(XjJdbcSinkConfig config) {
        return "truncate table " + tableWithSchema(config);
    }

    default String insertSql(XjJdbcSinkConfig config, List<ColumnMapper> columnMappers) {
        String columns =
                columnMappers.stream()
                        .map(m -> quoteIdentifier(m.getSinkColumnName()))
                        .collect(Collectors.joining(", "));
        String placeholders =
                columnMappers.stream()
                        .map(m -> m.getValueSupplier().get())
                        .collect(Collectors.joining(", "));
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)", tableWithSchema(config), columns, placeholders);
    }

    /** Read the real DB type name for each sink column, keyed case-insensitively. */
    default Map<String, String> sinkColumnDbTypes(
            Connection conn, XjJdbcSinkConfig config, List<String> sinkColumns)
            throws SQLException {
        String columns =
                sinkColumns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String sql =
                String.format("select %s from %s where 1=2", columns, tableWithSchema(config));
        Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            // Execute first, then read metadata. Some drivers (e.g. ClickHouse native
            // driver) only expose column metadata after the statement has been executed;
            // calling getMetaData() before executeQuery() returns null or fails.
            ps.executeQuery();
            ResultSetMetaData metaData = ps.getMetaData();
            if (metaData != null) {
                fillTypes(metaData, result);
            }
        }
        return result;
    }

    static void fillTypes(ResultSetMetaData metaData, Map<String, String> result)
            throws SQLException {
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            result.put(metaData.getColumnLabel(i), metaData.getColumnTypeName(i));
            result.put(metaData.getColumnName(i), metaData.getColumnTypeName(i));
        }
    }

    /**
     * Type-aware binding: primarily driven by the runtime java type of the value (with {@code
     * sourceSqlType} as an auxiliary hint), falling back to {@code setString} when the typed
     * binding is rejected by the target column. The {@code sinkDbType} is available for dialect
     * specific special handling.
     */
    default void bindValue(
            PreparedStatement ps,
            int index,
            Object value,
            SqlType sourceSqlType,
            String sinkDbType)
            throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.NULL);
            return;
        }
        try {
            if (value instanceof Integer) {
                ps.setInt(index, (Integer) value);
            } else if (value instanceof Short) {
                ps.setShort(index, (Short) value);
            } else if (value instanceof Byte) {
                ps.setByte(index, (Byte) value);
            } else if (value instanceof Long) {
                ps.setLong(index, (Long) value);
            } else if (value instanceof BigDecimal) {
                ps.setBigDecimal(index, (BigDecimal) value);
            } else if (value instanceof Double) {
                ps.setDouble(index, (Double) value);
            } else if (value instanceof Float) {
                ps.setFloat(index, (Float) value);
            } else if (value instanceof Boolean) {
                ps.setBoolean(index, (Boolean) value);
            } else if (value instanceof LocalDate) {
                ps.setDate(index, Date.valueOf((LocalDate) value));
            } else if (value instanceof LocalDateTime) {
                ps.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
            } else if (value instanceof LocalTime) {
                ps.setTime(index, Time.valueOf((LocalTime) value));
            } else if (value instanceof byte[]) {
                ps.setBytes(index, (byte[]) value);
            } else if (value instanceof String) {
                ps.setString(index, (String) value);
            } else {
                ps.setObject(index, value);
            }
        } catch (SQLException e) {
            ps.setString(index, Util.object2String(value));
        }
    }
}
