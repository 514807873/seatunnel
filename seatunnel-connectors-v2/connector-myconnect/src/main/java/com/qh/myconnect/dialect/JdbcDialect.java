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

package com.qh.myconnect.dialect;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.qh.myconnect.config.MidCount;
import com.qh.myconnect.config.PreConfig;
import com.qh.myconnect.config.SeaTunnelJobsHistoryErrorRecord;
import com.qh.myconnect.config.Util;
import com.qh.myconnect.dialect.ClickHouse.ClickHouseDialect;
import com.qh.myconnect.dialect.oracle.OracleDialect;
import com.qh.myconnect.dialect.pgsql.PostgresDialect;

import com.qh.myconnect.dialect.sqlserver.SqlServerDialect;
import org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.converter.JdbcRowConverter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 */
public interface JdbcDialect extends Serializable {
    Logger log = LoggerFactory.getLogger(JdbcDialect.class);

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();

    /**
     * Get converter that convert jdbc object to seatunnel internal object.
     *
     * @return a row converter for the database
     */
    JdbcRowConverter getRowConverter();

    /**
     * get jdbc meta-information type to seatunnel data type mapper.
     *
     * @return a type mapper for the database
     */
    JdbcDialectTypeMapper getJdbcDialectTypeMapper();

    /**
     * Quotes the identifier for table name or field name
     */
    default String quoteIdentifier(String identifier) {
        return identifier;
    }

    default String tableIdentifier(String database, String tableName) {
        return quoteIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    /**
     * Constructs the dialects insert statement for a single row. The returned string will be used
     * as a {@link PreparedStatement}. Fields in the statement must be in the same order as the
     * {@code fieldNames} parameter.
     *
     * <pre>{@code
     * INSERT INTO table_name (column_name [, ...]) VALUES (value [, ...])
     * }</pre>
     *
     * @return the dialects {@code INSERT INTO} statement.
     */
    default String getInsertIntoStatement(String database, String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName)
                        .collect(Collectors.joining(", "));
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableIdentifier(database, tableName), columns, placeholders);
    }

    /**
     * Constructs the dialects update statement for a single row with the given condition. The
     * returned string will be used as a {@link PreparedStatement}. Fields in the statement must be
     * in the same order as the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * UPDATE table_name SET col = val [, ...] WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code UPDATE} statement.
     */
    default String getUpdateStatement() {
        return "#set($separator = '') "
               + "#set($separator2 = '') "
               + "update ${table} set "
               + "#foreach( $item in $columns )"
               + " $separator  $item = ?"
               + "#set($separator = ', ') "
               + "#end"
               + " where "
               + "#foreach( $item in $pks )"
               + " $separator2  $item = ?"
               + "#set($separator2 = ' and  ') "
               + "#end";
    }

    /**
     * Constructs the dialects delete statement for a single row with the given condition. The
     * returned string will be used as a {@link PreparedStatement}. Fields in the statement must be
     * in the same order as the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * DELETE FROM table_name WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code DELETE} statement.
     */
    default String getDeleteStatement(String database, String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "DELETE FROM %s WHERE %s", tableIdentifier(database, tableName), conditionClause);
    }

    /**
     * Generates a query to determine if a row exists in the table. The returned string will be used
     * as a {@link PreparedStatement}.
     *
     * <pre>{@code
     * SELECT 1 FROM table_name WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code QUERY} statement.
     */
    default String getRowExistsStatement(
            String database, String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(field -> format("%s = :%s", quoteIdentifier(field), field))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "SELECT 1 FROM %s WHERE %s",
                tableIdentifier(database, tableName), fieldExpressions);
    }

    /**
     * Constructs the dialects upsert statement if supported; such as MySQL's {@code DUPLICATE KEY
     * UPDATE}, or PostgreSQL's {@code ON CONFLICT... DO UPDATE SET..}.
     *
     * <p>If supported, the returned string will be used as a {@link PreparedStatement}. Fields in
     * the statement must be in the same order as the {@code fieldNames} parameter.
     *
     * <p>If the dialect does not support native upsert statements, the writer will fallback to
     * {@code SELECT ROW Exists} + {@code UPDATE}/{@code INSERT} which may have poor performance.
     *
     * @return the dialects {@code UPSERT} statement or {@link Optional#empty()}.
     */
    Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields);

    /**
     * Different dialects optimize their PreparedStatement
     *
     * @return The logic about optimize PreparedStatement
     */
    default PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        }
        return statement;
    }

    default ResultSetMetaData getResultSetMetaData(Connection conn, JdbcSinkConfig jdbcSourceConfig)
            throws SQLException {
        String table = jdbcSourceConfig.getTable();
        Map<String, String> fieldMapper = jdbcSourceConfig.getFieldMapper();
        List<String> columns = new ArrayList<>();
        fieldMapper.forEach(
                (k, v) -> {
                    columns.add(v);
                });
        String sql =
                String.format(
                        "select  %s from %s where 1=2 ", StringUtils.join(columns, ","), table);
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.executeQuery();
        return ps.getMetaData();
    }

    default String getTableCountSql(JdbcSinkConfig jdbcSinkConfig) {
        String table = jdbcSinkConfig.getTable();
        if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
            return String.format("select  count(1) from %s.%s ", jdbcSinkConfig.getDbSchema(), table);
        }
        return String.format("select  count(1) from %s ", table);
    }

    default String updateTableSql(JdbcSinkConfig jdbcSinkConfig, String columnName, List<String> ucColumns) {
        String operateColumnName = jdbcSinkConfig.getPreConfig().getRecordOperateColumnName();
        String timestampColumnName = jdbcSinkConfig.getPreConfig().getAutoTimestampColumnName();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String timestampValue = now.format(formatter);
        String tmpSql = "";
        if (!jdbcSinkConfig.getPreConfig().isOpenDelete()
            && jdbcSinkConfig.getPreConfig().isRecordOperate()
            && jdbcSinkConfig.getPreConfig().isAutoTimestamp()
        ) {
            tmpSql += String.format(",%s='%s',%s='%s'", operateColumnName, "U", timestampColumnName, timestampValue);
        }

        if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
            return "update " +
                   jdbcSinkConfig.getDbSchema() + "." +
                   quoteIdentifier(jdbcSinkConfig.getTable()) +
                   " set " +
                   quoteIdentifier(columnName) +
                   " = ? " + tmpSql + " where " +
                   StringUtils.join(ucColumns.stream().map(x -> quoteIdentifier(x) + " =? ").collect(Collectors.toList()), " and ");
        }
        return "update " +
               quoteIdentifier(jdbcSinkConfig.getTable()) +
               " set " +
               quoteIdentifier(columnName) +
               " = ? " + tmpSql + " where " +
               StringUtils.join(ucColumns.stream().map(x -> quoteIdentifier(x) + " =? ").collect(Collectors.toList()), " and ");
    }

    default String updateTableSqlZipper(JdbcSinkConfig jdbcSinkConfig, List<String> ucColumns) {
        String columnName = jdbcSinkConfig.getPreConfig().getZipperColumns().get(2);
        String OPERATEFLAG = jdbcSinkConfig.getPreConfig().getZipperColumns().get(0);
        switch (jdbcSinkConfig.getDbType()) {
            case "PGSQL":
            case "MYSQL":
            case "SQLSERVER":
                columnName = columnName.toLowerCase();
                OPERATEFLAG = OPERATEFLAG.toLowerCase();
                break;
            case "ORACLE":
                columnName = columnName.toUpperCase();
                OPERATEFLAG = OPERATEFLAG.toUpperCase();
            default:
                break;
        }
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String currentTimeString = now.format(formatter);
        if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
            return "update " +
                   jdbcSinkConfig.getDbSchema() + "." +
                   quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName()) +
                   " set "
                   + quoteIdentifier(columnName) + " = " + "'" + currentTimeString + "'" +
                   " where " + quoteIdentifier(columnName) + " is null and " +
                   StringUtils.join(ucColumns.stream().map(x -> quoteIdentifier(x) + " =? ").collect(Collectors.toList()), " and ");
        }
        return "update " +
               quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName()) +
               " set "
               + quoteIdentifier(columnName) + " = " + "'" + currentTimeString + "'" +
               " where " + quoteIdentifier(columnName) + " is null and " +
               StringUtils.join(ucColumns.stream().map(x -> quoteIdentifier(x) + " =? ").collect(Collectors.toList()), " and ");
    }

    default String insertModifyTableSql(JdbcSinkConfig jdbcSinkConfig, String tmpTableName, List<String> columns,
                                        List<String> ucColumns) {
        String OPERATEFLAG = jdbcSinkConfig.getPreConfig().getZipperColumns().get(0);
        String OPERATETIME = jdbcSinkConfig.getPreConfig().getZipperColumns().get(1);
        switch (jdbcSinkConfig.getDbType()) {
            case "PGSQL":
            case "MYSQL":
            case "SQLSERVER":
                OPERATEFLAG = OPERATEFLAG.toLowerCase();
                OPERATETIME = OPERATETIME.toLowerCase();
                break;
            default:
                break;
        }
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String currentTimeString = now.format(formatter);
        List<String> collect1 = columns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        collect1.add(OPERATEFLAG);
        collect1.add(OPERATETIME);
        List<String> collect = columns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        List<String> collect2 =
                ucColumns.stream().map(this::quoteIdentifier).map(x -> x + " = ? ").collect(Collectors.toList());
        if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
            return String.format("insert "
                                 + " into "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName())
                                 + "(%s)"
                                 + "select "
                                 + " %s, "
                                 + "'U',"
                                 + "'" + currentTimeString + "'"
                                 + "from "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(tmpTableName)
                                 + " where "
                                 + " %s ",
                    StringUtils.join(collect1, ','),
                    StringUtils.join(collect, ','),
                    StringUtils.join(collect2, " and ")
            );
        }
        else {
            return String.format("insert "
                                 + " into "
                                 + quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName())
                                 + "(%s)"
                                 + "select "
                                 + " %s, "
                                 + "'U',"
                                 + "'" + currentTimeString + "'"
                                 + "from "
                                 + quoteIdentifier(tmpTableName)
                                 + " where "
                                 + " %s  ",
                    StringUtils.join(collect1, " , "),
                    StringUtils.join(collect, ","),
                    StringUtils.join(collect2, " and ")
            );
        }
    }


    default String deleteTableSql(JdbcSinkConfig jdbcSinkConfig, List<String> ucColumns) {
        if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
            return "delete from " + jdbcSinkConfig.getDbSchema() + "." + quoteIdentifier(jdbcSinkConfig.getTable())
                   + " where " +
                   StringUtils.join(ucColumns.stream().map(x -> quoteIdentifier(x) + " =? ").collect(Collectors.toList()), " and ");
        }
        return "delete from " + quoteIdentifier(jdbcSinkConfig.getTable())
               + " where " +
               StringUtils.join(ucColumns.stream().map(x -> quoteIdentifier(x) + " =? ").collect(Collectors.toList()), " and ");
    }

    default void setPreparedStatementValueByDbType(
            int position, PreparedStatement preparedStatement, String dbType, String value)
            throws SQLException {
        preparedStatement.setString(position, value);
    }

    default void setPreparedStatementValue(
            PreparedStatement preparedStatement, int position, Object value) throws SQLException {
        if (null != value) {
            if (value instanceof Date) {
                preparedStatement.setTimestamp(position, new Timestamp(((Date) value).getTime()));
            }
            else if (value instanceof LocalDate) {
                preparedStatement.setDate(position, java.sql.Date.valueOf((LocalDate) value));
            }
            else if (value instanceof Integer) {
                preparedStatement.setInt(position, (Integer) value);
            }
            else if (value instanceof Long) {
                preparedStatement.setLong(position, (Long) value);
            }
            else if (value instanceof Double) {
                preparedStatement.setDouble(position, (Double) value);
            }
            else if (value instanceof Float) {
                preparedStatement.setFloat(position, (Float) value);
            }
            else if (value instanceof LocalDateTime) {
                preparedStatement.setTimestamp(position, Timestamp.valueOf((LocalDateTime) value));
            }
            else if (value instanceof BigDecimal) {
                preparedStatement.setBigDecimal(position, (BigDecimal) value);
            }
            else {
                preparedStatement.setString(position, (String) value);
            }
        }
        else {
            preparedStatement.setNull(position, Types.NULL);
        }
    }

    default String getSinkQueryUpdate(
            List<ColumnMapper> columnMappers, int rowSize, JdbcSinkConfig jdbcSinkConfig) {
        List<ColumnMapper> ucColumns =
                columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString =
                " select <columns:{sub | <sub.sinkColumnName>}; separator=\", \"> "
                + "  from <table> a "
                + " where  ";
        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("table", jdbcSinkConfig.getTable());
        sqlQueryTemplate.add("columns", columnMappers);
        String sqlQuery = sqlQueryTemplate.render();
        List<String> where = new ArrayList<>();
        for (int i = 0; i < rowSize; i++) {
            String tmpWhere = "( <ucs:{uc | <uc.sinkColumnName> = ?  }; separator=\" and \">  )";
            ST tmpst = new ST(tmpWhere);
            tmpst.add("ucs", ucColumns);
            String render = tmpst.render();
            where.add(render);
        }
        String wheres = StringUtils.join(where, "  or ");
        if (rowSize == 0) {
            sqlQuery = sqlQuery + " 1=2";
        }
        else {
            sqlQuery = sqlQuery + wheres;
        }
        return sqlQuery;
    }

    default String insertTableSql(
            JdbcSinkConfig jdbcSinkConfig, List<String> columns, List<String> values) {
        String sql =
                "insert into "
                + jdbcSinkConfig.getTable()
                + String.format("(%s)", StringUtils.join(columns, ","))
                + String.format("values (%s)", StringUtils.join(values, ","));
        return sql;
    }

    default String insertTmpTableSql(
            JdbcSinkConfig jdbcSinkConfig, List<String> columns, List<String> values) {
        String sql =
                "insert into "
                + "XJ$_" + jdbcSinkConfig.getTable()
                + String.format("(%s)", StringUtils.join(columns, ","))
                + String.format("values (%s)", StringUtils.join(values, ","));
        return sql;
    }

    default String insertTableOnlyColumn(
            JdbcSinkConfig jdbcSinkConfig, List<String> columns) {
        return null;
    }

    default String getSinkQueryZipper(
            List<ColumnMapper> columnMappers, int rowSize, JdbcSinkConfig jdbcSinkConfig) {
        List<ColumnMapper> ucColumns =
                columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString =
                " select <columns:{sub | <sub.sinkColumnName>}; separator=\", \"> "
                + "  from <table> a "
                + " where  ";
        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("table", jdbcSinkConfig.getTable());
        sqlQueryTemplate.add("columns", columnMappers);
        String sqlQuery = sqlQueryTemplate.render();
        List<String> where = new ArrayList<>();
        for (int i = 0; i < rowSize; i++) {
            String tmpWhere =
                    "( <ucs:{uc | <uc.sinkColumnName> = ?  }; separator=\" and \"> and ZIPPERFLAG='N' )";
            ST tmpst = new ST(tmpWhere);
            tmpst.add("ucs", ucColumns);
            String render = tmpst.render();
            where.add(render);
        }
        String wheres = StringUtils.join(where, "  or ");
        if (rowSize == 0) {
            sqlQuery = sqlQuery + " 1=2";
        }
        else {
            sqlQuery = sqlQuery + wheres;
        }
        return sqlQuery;
    }

    default String copyTableOnlyColumn(
            String sourceTable, String targetTable, JdbcSinkConfig jdbcSinkConfig) {
        return format(
                "create  table %s as select * from %s where 1=2 ",
                targetTable,
//                StringUtils.join(jdbcSinkConfig.getPrimaryKeys().stream().map(x -> "`" + x + "`").collect(Collectors.toList()), ','),
                "`" + sourceTable + "`");
    }

    default String copyTableOnlyColumnOnCluster(
            String sourceTable,
            String targetTable,
            JdbcSinkConfig jdbcSinkConfig,
            String clusterName,
            String dataBase) {
        return String.format(
                "create  table %s on CLUSTER %s ENGINE=ReplicatedMergeTree() order by (%s) settings "
                + "allow_nullable_key=1 as select * "
                + "from  %s.%s "
                + "where 1=2 ",
                "`" + targetTable + "`",
                clusterName,
//                StringUtils.join(jdbcSinkConfig.getPrimaryKeys().stream().map(x -> "`" + x + "`").collect(Collectors.toList()), ','),
                StringUtils.join(jdbcSinkConfig.getPrimaryKeys().stream().map(x -> "`" + x + "`").collect(Collectors.toList()), ','),
                dataBase,
                "`" + sourceTable + "`");
    }

    default String truncateTable(JdbcSinkConfig jdbcSinkConfig) {
        return String.format("truncate  table %s", jdbcSinkConfig.getTable());
    }

    default String dropTable(JdbcSinkConfig jdbcSinkConfig, String tableName) {
        return String.format("drop table  %s", "`" + tableName + "`");
    }

    default String dropTableOnCluster(
            JdbcSinkConfig jdbcSinkConfig, String database, String tableName, String clusterName) {
        return String.format(
                "drop table  %s.%s on cluster %s no delay ", database, "`" + tableName + "`", clusterName);
    }

    default String createIndex(String tmpTableName, JdbcSinkConfig jdbcSinkConfig) {
        return String.format(
                "CREATE UNIQUE INDEX %s ON %s(%s)",
                tmpTableName, tmpTableName, StringUtils.join(jdbcSinkConfig.getPrimaryKeys(), ','));
    }

    default void deleteReInsertData(
            Connection connection,
            String table,
            String ucTable,
            List<ColumnMapper> ucColumns,
            JdbcSinkConfig jdbcSinkConfig) {
        String operateColumnName = jdbcSinkConfig.getPreConfig().getRecordOperateColumnName();
        String delSql =
                "DELETE  FROM <table> WHERE (<pks:{pk | <pk.sinkColumnName>}; separator=\", \">) IN   (SELECT  <pks:{pk | <pk.sinkColumnName>}; separator=\", \"> FROM <ucTable>  ) "
                + " and " + operateColumnName + "='D'";
        if (this instanceof ClickHouseDialect) {
            delSql =
                    "ALTER  TABLE <table> DELETE  WHERE (<pks:{pk | <pk.sinkColumnName>}; separator=\", \">) IN   (SELECT  <pks:{pk | <pk.sinkColumnName>}; separator=\", \"> FROM <ucTable>  ) "
                    + " and " + operateColumnName + "='D' ";
            if (StringUtils.isNoneBlank(jdbcSinkConfig.getPreConfig().getClusterName())) {
                delSql += " SETTINGS allow_nondeterministic_mutations = 1 ";
            }
        }
        ST template = new ST(delSql);
        if (StringUtils.isNoneBlank(jdbcSinkConfig.getDbSchema())) {
            template.add("table", jdbcSinkConfig.getDbSchema() + "." + table);
            template.add("ucTable", jdbcSinkConfig.getDbSchema() + "." + ucTable);
        }
        else {
            template.add("table", table);
            template.add("ucTable", ucTable);
        }
        template.add("pks", ucColumns);
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(template.render());
            preparedStatement.executeUpdate();
            preparedStatement.close();
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    default int deleteData(
            Connection connection, String table, String ucTable, List<ColumnMapper> ucColumns) {
        String delSql =
                "delete from  <table>    "
                + " where not exists "
                + "       (select  <pks:{pk | <pk.sinkColumnName>}; separator=\" , \"> from <tmpTable> where <pks:{pk | <table>.<pk.sinkColumnName>=<tmpTable>.<pk.sinkColumnName> }; separator=\" and \">  ) ";
        ST template = new ST(delSql);
        template.add("table", table);
        template.add("tmpTable", ucTable);
        template.add("pks", ucColumns);
        PreparedStatement preparedStatement = null;
        int del = 0;
        try {
            preparedStatement = connection.prepareStatement(template.render());
            del = preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return del;
    }

    default int deleteDataLogic(
            Connection connection,
            String table,
            String ucTable,
            List<ColumnMapper> ucColumns,
            PreConfig preConfig) {
        String operateColumnName = preConfig.getRecordOperateColumnName();
        String timestampColumnName = preConfig.getAutoTimestampColumnName();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String timestampValue = now.format(formatter);
        String delSql =
                "update  <table>    "
                + " set <operateColumnName>='D',<timestampColumnName>='<timestampValue>'"
                + " where not exists "
                + "       (select  <pks:{pk | <pk.sinkColumnName>}; separator=\" , \"> from <tmpTable> where <pks:{pk | <table>.<pk.sinkColumnName>=<tmpTable>.<pk.sinkColumnName> }; separator=\" and \">  ) "
                + " and <operateColumnName> !='D'";
        ST template = new ST(delSql);
        template.add("table", table);
        template.add("tmpTable", ucTable);
        template.add("pks", ucColumns);
        template.add("operateColumnName", operateColumnName);
        template.add("timestampColumnName", timestampColumnName);
        template.add("timestampValue", timestampValue);
        PreparedStatement preparedStatement = null;
        int del = 0;
        try {
            preparedStatement = connection.prepareStatement(template.render());
            del = preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return del;
    }

    default int deleteDataZipper(
            JdbcSinkConfig jdbcSinkConfig,
            Connection connection,
            String zipperTable,
            String originTable,
            List<ColumnMapper> columnMappers,
            List<ColumnMapper> ucColumns) {
        String OPERATEFLAG = jdbcSinkConfig.getPreConfig().getZipperColumns().get(0);
        String OPERATETIME = jdbcSinkConfig.getPreConfig().getZipperColumns().get(1);
        String OPERATETIME_END = jdbcSinkConfig.getPreConfig().getZipperColumns().get(2);
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String currentTimeString = now.format(formatter);
        switch (jdbcSinkConfig.getDbType()) {
            case "PGSQL":
            case "MYSQL":
            case "SQLSERVER":
            case "TRINO":
                OPERATEFLAG = OPERATEFLAG.toLowerCase();
                OPERATETIME = OPERATETIME.toLowerCase();
                OPERATETIME_END = OPERATETIME_END.toLowerCase();
                break;
            case "ORACLE":
                OPERATEFLAG = OPERATEFLAG.toUpperCase();
                OPERATETIME = OPERATETIME.toUpperCase();
                OPERATETIME_END = OPERATETIME_END.toUpperCase();
                break;
            default:
                break;
        }
        List<String> columns = columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        List<String> newColumns = columns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        List<String> allColumns = columns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        allColumns.add(quoteIdentifier(OPERATEFLAG));
        allColumns.add(quoteIdentifier(OPERATETIME));
        allColumns.add(quoteIdentifier(OPERATETIME_END));
        String insertDelSql =
                "insert into  <table> (<allColumns>) "
                + "select <columns>,"
                + " 'D' " + OPERATEFLAG + ","
                + "'" + currentTimeString + "' " + OPERATETIME + ", "
                + "'" + currentTimeString + "' " + OPERATETIME_END + " "
                + " from (select * from  <table> where " + OPERATETIME_END + " IS NULL and " + OPERATEFLAG + " in ('I','U') " + " ) a "
                + "   WHERE (<pks:{pk | <pk.sinkColumnName>}; "
                + "separator=\", "
                + "\">) NOT IN   (SELECT  <pks:{pk | <pk.sinkColumnName>}; separator=\", \"> FROM "
                + "<ucTable>  ) ";
        ST template1 = new ST(insertDelSql);
        template1.add("table", zipperTable);
        template1.add("ucTable", originTable);
        template1.add("columns", StringUtils.join(newColumns, ","));
        template1.add("allColumns", StringUtils.join(allColumns, ","));
        template1.add("pks", ucColumns);
        String render = template1.render();
        try {
            connection.createStatement().execute(render);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        String delSql =
                "update  <table>    "
                + " set " + OPERATETIME_END + "='" + currentTimeString + "'"
                + " where not exists "
                + "       (select  <pks:{pk | <pk.sinkColumnName>}; separator=\" , \"> from <tmpTable> where <pks:{pk | <table>.<pk.sinkColumnName>=<tmpTable>.<pk.sinkColumnName> }; separator=\" and \">  ) "
                + " and " + OPERATETIME_END + " IS NULL and " + OPERATEFLAG + " in ('I','U') ";
        ST template = new ST(delSql);
        template.add("table", zipperTable);
        template.add("tmpTable", originTable);
        template.add("pks", ucColumns);
        PreparedStatement preparedStatement = null;
        int del = 0;
        try {
            preparedStatement = connection.prepareStatement(template.render());
            del = preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return del;
    }

    default int deleteDataZipperCluster(
            JdbcSinkConfig jdbcSinkConfig,
            Connection connection,
            String table,
            String ucTable,
            List<ColumnMapper> columnMappers,
            List<ColumnMapper> ucColumns,
            String clusterName) {

        return 0;
    }

    default String insertDataCount(JdbcSinkConfig jdbcSinkConfig, String tmpTableName, List<String> ucColumns) {
        String operateColumnName = jdbcSinkConfig.getPreConfig().getRecordOperateColumnName();
        String timestampColumnName = jdbcSinkConfig.getPreConfig().getAutoTimestampColumnName();
        String tmpSql = " where 1=1 ";
        if (!jdbcSinkConfig.getPreConfig().isOpenDelete()
            && jdbcSinkConfig.getPreConfig().isRecordOperate()
            && jdbcSinkConfig.getPreConfig().isAutoTimestamp()
        ) {
            tmpSql += " and (" + operateColumnName + " != 'D' or " + operateColumnName + " is null)";
        }

        List<String> collect = ucColumns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
            return String.format("select "
                                 + " count(1) "
                                 + "from "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(tmpTableName)
                                 + " where "
                                 + " (%s) not in ( "
                                 + " select "
                                 + " %s "
                                 + " from "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(jdbcSinkConfig.getTable())
                                 + tmpSql
                                 + ")", StringUtils.join(collect, ','),
                    StringUtils.join(collect, ',')
            );
        }
        else {
            return String.format("select "
                                 + " count(1) "
                                 + " from "
                                 + quoteIdentifier(tmpTableName)
                                 + " where "
                                 + " (%s) not in ( "
                                 + " select "
                                 + " %s "
                                 + " from "
                                 + quoteIdentifier(jdbcSinkConfig.getTable())
                                 + tmpSql
                                 + ")", StringUtils.join(collect, ','),
                    StringUtils.join(collect, ',')
            );
        }
    }

    default String insertDataCountZipper(JdbcSinkConfig jdbcSinkConfig, String tmpTableName, List<String> ucColumns) {
        List<String> collect = ucColumns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        String OPERATEFLAG = jdbcSinkConfig.getPreConfig().getZipperColumns().get(0);
        String OPERATETIME_END = jdbcSinkConfig.getPreConfig().getZipperColumns().get(2);
        switch (jdbcSinkConfig.getDbType()) {
            case "PGSQL":
            case "MYSQL":
            case "SQLSERVER":
                OPERATEFLAG = OPERATEFLAG.toLowerCase();
                OPERATETIME_END = OPERATETIME_END.toLowerCase();
                break;
            case "ORACLE":
                OPERATEFLAG = OPERATEFLAG.toUpperCase();
                OPERATETIME_END = OPERATETIME_END.toUpperCase();

            default:
                break;
        }
        if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
            return String.format("select "
                                 + " count(1) "
                                 + "from "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(tmpTableName)
                                 + " where "
                                 + " (%s) not in ( "
                                 + " select "
                                 + " %s "
                                 + " from "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName())
                                 + " where " + OPERATETIME_END + " is null  and " + OPERATEFLAG + " in ('I','U') "
                                 + ")", StringUtils.join(collect, ','),
                    StringUtils.join(collect, ',')
            );
        }
        else {
            return String.format("select "
                                 + " count(1) "
                                 + " from "
                                 + quoteIdentifier(tmpTableName)
                                 + " where "
                                 + " (%s) not in ( "
                                 + " select "
                                 + " %s "
                                 + " from "
                                 + quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName())
                                 + " where " + OPERATETIME_END + " is null  and " + OPERATEFLAG + " in ('I','U') "
                                 + ")", StringUtils.join(collect, ','),
                    StringUtils.join(collect, ',')
            );
        }
    }

    default String insertData(JdbcSinkConfig jdbcSinkConfig, String tmpTableName, List<String> columns, List<String> ucColumns) {
        List<String> collect1 = columns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        List<String> collectCopy = new ArrayList<>(collect1);
        List<String> collect2 = ucColumns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        String operateColumnName = jdbcSinkConfig.getPreConfig().getRecordOperateColumnName();
        String timestampColumnName = jdbcSinkConfig.getPreConfig().getAutoTimestampColumnName();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String timestampValue = now.format(formatter);
        String tmpSql = " where 1=1 ";
        if (!jdbcSinkConfig.getPreConfig().isOpenDelete()
            && jdbcSinkConfig.getPreConfig().isRecordOperate()
            && jdbcSinkConfig.getPreConfig().isAutoTimestamp()
        ) {
            tmpSql += " and (" + operateColumnName + " != 'D' or " + operateColumnName + " is null)";
            collect1.add(operateColumnName);
            collect1.add(timestampColumnName);
            collectCopy.add(" 'I' " + operateColumnName);
            collectCopy.add("'" + timestampValue + "'" + timestampColumnName);
        }
        if (StringUtils.isNoneBlank(jdbcSinkConfig.getDbSchema())) {
            return String.format("insert "
                                 + " into "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(jdbcSinkConfig.getTable())
                                 + "(%s)"
                                 + "select "
                                 + " %s "
                                 + "from "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(tmpTableName)
                                 + "where "
                                 + " (%s) not in ( "
                                 + " select "
                                 + "  %s "
                                 + " from "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(jdbcSinkConfig.getTable())
                                 + tmpSql
                                 + ")",
                    StringUtils.join(collect1, ','),
                    StringUtils.join(collectCopy, ','),
                    StringUtils.join(collect2, ','),
                    StringUtils.join(collect2, ',')
            );
        }
        else {
            return String.format("insert "
                                 + " into "
                                 + quoteIdentifier(jdbcSinkConfig.getTable())
                                 + "(%s)"
                                 + "select "
                                 + " %s "
                                 + "from "
                                 + quoteIdentifier(tmpTableName)
                                 + "where "
                                 + " (%s) not in ( "
                                 + " select "
                                 + "  %s "
                                 + " from "
                                 + quoteIdentifier(jdbcSinkConfig.getTable())
                                 + tmpSql
                                 + ")",
                    StringUtils.join(collect1, ','),
                    StringUtils.join(collectCopy, ','),
                    StringUtils.join(collect2, ','),
                    StringUtils.join(collect2, ',')
            );
        }
    }

    default String insertDataZipper(JdbcSinkConfig jdbcSinkConfig, String tmpTableName, List<String> columns,
                                    List<String> ucColumns) {
        String OPERATEFLAG = jdbcSinkConfig.getPreConfig().getZipperColumns().get(0);
        String OPERATETIME = jdbcSinkConfig.getPreConfig().getZipperColumns().get(1);
        String OPERATETIME_END = jdbcSinkConfig.getPreConfig().getZipperColumns().get(2);
        switch (jdbcSinkConfig.getDbType()) {
            case "PGSQL":
            case "MYSQL":
            case "SQLSERVER":
                OPERATEFLAG = OPERATEFLAG.toLowerCase();
                OPERATETIME = OPERATETIME.toLowerCase();
                OPERATETIME_END = OPERATETIME_END.toLowerCase();
                break;
            case "ORACLE":
                OPERATEFLAG = OPERATEFLAG.toUpperCase();
                OPERATETIME = OPERATETIME.toUpperCase();
                OPERATETIME_END = OPERATETIME_END.toUpperCase();
            default:
                break;
        }
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String currentTimeString = now.format(formatter);
        List<String> collect1 = columns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        collect1.add(OPERATEFLAG);
        collect1.add(OPERATETIME);
        List<String> collect = columns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        List<String> collect2 = ucColumns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
        if (jdbcSinkConfig.getDbSchema() != null && !jdbcSinkConfig.getDbSchema().isEmpty()) {
            return String.format("insert "
                                 + " into "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName())
                                 + "(%s)"
                                 + " select "
                                 + " %s, "
                                 + "'I',"
                                 + "'" + currentTimeString + "'"
                                 + "from "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(tmpTableName)
                                 + "where "
                                 + " (%s) not in ( "
                                 + " select "
                                 + "  %s "
                                 + " from "
                                 + quoteIdentifier(jdbcSinkConfig.getDbSchema())
                                 + "."
                                 + quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName())
                                 + " where " + OPERATETIME_END + " is null and " + OPERATEFLAG + " in ('I','U') "
                                 + ")",
                    StringUtils.join(collect1, ','),
                    StringUtils.join(collect, ','),
                    StringUtils.join(collect2, ','),
                    StringUtils.join(collect2, ',')
            );
        }
        else {
            return String.format("insert "
                                 + " into "
                                 + quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName())
                                 + "(%s)"
                                 + "select "
                                 + " %s, "
                                 + "'I',"
                                 + "'" + currentTimeString + "'"
                                 + "from "
                                 + quoteIdentifier(tmpTableName)
                                 + "where "
                                 + " (%s) not in ( "
                                 + " select "
                                 + "  %s "
                                 + " from "
                                 + quoteIdentifier(jdbcSinkConfig.getPreConfig().getZipperTableName())
                                 + " where " + OPERATETIME_END + " is null  and " + OPERATEFLAG + " in ('I','U') "
                                 + ")",
                    StringUtils.join(collect1, ','),
                    StringUtils.join(collect, ','),
                    StringUtils.join(collect2, ','),
                    StringUtils.join(collect2, ',')
            );
        }
    }


    default int deleteDataOnCluster(
            Connection connection,
            String table,
            String ucTable,
            List<ColumnMapper> ucColumns,
            String clusterName) {

        return 0;
    }

    default int deleteDataOnClusterLogic(
            Connection connection,
            String table,
            String ucTable,
            List<ColumnMapper> ucColumns,
            String clusterName,
            PreConfig preConfig) {

        return 0;
    }

    default Long getTableCount(Connection connection, String table) {
        long count = 0L;
        try {
            PreparedStatement preparedStatement =
                    connection.prepareStatement(format("select  count(1) sl  from %s", table));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                count = resultSet.getLong("sl");
                preparedStatement.close();
            }
            return count;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    default Long getTableCount(Connection connection, String schema, String table) {
        long count = 0L;
        try {
            PreparedStatement preparedStatement =
                    connection.prepareStatement(format("select  count(1) sl  from \"%s\".\"%s\"", schema, table));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                count = resultSet.getLong("sl");
                preparedStatement.close();
            }
            return count;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    default String getDataSql(JdbcSinkConfig jdbcSinkConfig, List<ColumnMapper> columnMappers, String tableName) {
        String operateColumnName = jdbcSinkConfig.getPreConfig().getRecordOperateColumnName();
        String timestampColumnName = jdbcSinkConfig.getPreConfig().getAutoTimestampColumnName();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String timestampValue = now.format(formatter);
        String tmpSql = "";
        if (!jdbcSinkConfig.getPreConfig().isOpenDelete()
            && jdbcSinkConfig.getPreConfig().isRecordOperate()
            && jdbcSinkConfig.getPreConfig().isAutoTimestamp()
            && jdbcSinkConfig.getTable().equalsIgnoreCase(tableName)
        ) {
            tmpSql += " where  (" + operateColumnName + " != 'D' or " + operateColumnName + " is null)";
        }

        List<String> columns = columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        List<String> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        List<String> newColumns = new ArrayList<>();
        List<String> newUcColumns = new ArrayList<>();
        for (String column : columns) {
            newColumns.add(quoteIdentifier(column));
        }
        for (String column : ucColumns) {
            newUcColumns.add(quoteIdentifier(column));
        }
        if (this instanceof OracleDialect || this instanceof PostgresDialect || this instanceof SqlServerDialect) {
            return String.format("select %s from %s.%s %s order by %s",
                    StringUtils.join(newColumns, ","),
                    jdbcSinkConfig.getDbSchema(),
                    quoteIdentifier(tableName),
                    tmpSql,
                    StringUtils.join(newUcColumns, ",")
            );
        }
        else {
            return String.format("select %s from %s %s order by %s",
                    StringUtils.join(newColumns, ","),
                    quoteIdentifier(tableName),
                    tmpSql,
                    StringUtils.join(newUcColumns, ",")
            );
        }
    }

    default String getDataSqlZipper(JdbcSinkConfig jdbcSinkConfig, List<ColumnMapper> columnMappers, String tableName) {
        String OPERATEFLAG = jdbcSinkConfig.getPreConfig().getZipperColumns().get(0);
        String OPERATETIME_END = jdbcSinkConfig.getPreConfig().getZipperColumns().get(2);
        switch (jdbcSinkConfig.getDbType()) {
            case "PGSQL":
            case "MYSQL":
            case "SQLSERVER":
                OPERATETIME_END = OPERATETIME_END.toLowerCase();
                break;
            case "ORACLE":
                OPERATETIME_END = OPERATETIME_END.toUpperCase();
            default:
                break;
        }
        List<String> columns = columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        List<String> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        List<String> newColumns = new ArrayList<>();
        List<String> newUcColumns = new ArrayList<>();
        for (String column : columns) {
            newColumns.add(quoteIdentifier(column));
        }
        for (String column : ucColumns) {
            newUcColumns.add(quoteIdentifier(column));
        }
        if (this instanceof OracleDialect || this instanceof PostgresDialect || this instanceof SqlServerDialect) {
            return String.format("select %s from %s.%s where %s is null and %s in ('I','U') order by %s",
                    StringUtils.join(newColumns, ","),
                    jdbcSinkConfig.getDbSchema(),
                    quoteIdentifier(tableName),
                    OPERATETIME_END,
                    OPERATEFLAG,
                    StringUtils.join(newUcColumns, ",")
            );
        }
        else {
            return String.format("select %s from %s  where %s is null and %s in ('I','U') order by %s",
                    StringUtils.join(newColumns, ","),
                    quoteIdentifier(tableName),
                    OPERATETIME_END,
                    OPERATEFLAG,
                    StringUtils.join(newUcColumns, ",")
            );
        }
    }

    default void insertToDb(List<ColumnMapper> columnMappers,
                            JdbcSinkConfig jdbcSinkConfig,
                            Connection conn,
                            Map<String, String> metaDataHash,
                            List<SeaTunnelRow> seaTunnelRows,
                            Util util,
                            JobContext jobContext,
                            Set<String> sqlErrorType,
                            MidCount midCount
    ) {
        Long tmpInsertCount = null;
        String sql = null;
        try {
            List<String> columns = columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
            List<String> values = columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            sql = this.insertTableSql(jdbcSinkConfig, columns, values);
            PreparedStatement psUpsert = conn.prepareStatement(sql);
            tmpInsertCount = midCount.getInsertCount();
            boolean hasError = false;
            Exception exception = null;
            for (SeaTunnelRow seaTunnelRow : seaTunnelRows) {
                if (seaTunnelRow != null) {
                    for (int i = 0; i < columnMappers.size(); i++) {
                        Integer valueIndex = columnMappers.get(i).getSourceRowPosition();
                        Object field = columnMappers.get(i).getConverter().apply(seaTunnelRow.getField(valueIndex));
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        this.setPreparedStatementValueByDbType(i + 1, psUpsert, dbType, util.Object2String(field));
                    }
                    midCount.setInsertCount(midCount.getInsertCount() + 1);
                    try {
                        psUpsert.addBatch();
                    } catch (SQLException e) {
                        hasError = true;
                        exception = e;
                        break;
                    }
                }
            }

            if (hasError) {
//                throw new RuntimeException();
                log.error("批量插入错误:", exception);
            }
            psUpsert.executeBatch();
            conn.commit();
            psUpsert.clearBatch();
            psUpsert.close();

        } catch (Exception e) {
            log.error("错误sql:" + sql, e);
            try {
                conn.rollback();
                midCount.setInsertCount(tmpInsertCount);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            insertToDbOneByOne(columnMappers,
                    jdbcSinkConfig,
                    conn,
                    metaDataHash,
                    seaTunnelRows,
                    util,
                    jobContext,
                    sqlErrorType,
                    midCount);
        }
    }

    default void insertToDbOneByOne(List<ColumnMapper> columnMappers,
                                    JdbcSinkConfig jdbcSinkConfig,
                                    Connection conn,
                                    Map<String, String> metaDataHash,
                                    List<SeaTunnelRow> seaTunnelRows,
                                    Util util,
                                    JobContext jobContext,
                                    Set<String> sqlErrorType,
                                    MidCount midCount) {
        try {
            List<String> columns = columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
            List<String> values = columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            String sql = this.insertTableSql(jdbcSinkConfig, columns, values);
            for (SeaTunnelRow seaTunnelRow : seaTunnelRows) {
                if (seaTunnelRow != null) {
                    PreparedStatement psUpsert = conn.prepareStatement(sql);
                    for (int i = 0; i < columnMappers.size(); i++) {
                        Integer valueIndex = columnMappers.get(i).getSourceRowPosition();
                        Object field = columnMappers.get(i).getConverter().apply(seaTunnelRow.getField(valueIndex));
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        this.setPreparedStatementValueByDbType(i + 1, psUpsert, dbType, util.Object2String(field));
                    }
                    try {
                        psUpsert.addBatch();
                        psUpsert.executeBatch();
                        conn.commit();
                        psUpsert.clearBatch();
                        psUpsert.close();
                        midCount.setInsertCount(midCount.getInsertCount() + 1);
                    } catch (SQLException ee) {
                        midCount.setErrorCount(midCount.getErrorCount() + 1);
                        if (jobContext.getIsRecordErrorData() == 1 && midCount.getErrorCount() <= jobContext.getMaxRecordNumber() && !sqlErrorType.contains(ee.getMessage())) {
                            LinkedHashMap<String, Object> jsonObject = new LinkedHashMap<>();
                            for (int i = 0; i < columnMappers.size(); i++) {
                                jsonObject.put(columnMappers.get(i).getSourceColumnName(), seaTunnelRow.getField(i));
                            }
                            log.info(JSON.toJSONString(jsonObject, JSONWriter.Feature.WriteMapNullValue, JSONWriter.Feature.WriteNullListAsEmpty));
                            SeaTunnelJobsHistoryErrorRecord errorRecord = new SeaTunnelJobsHistoryErrorRecord();
                            errorRecord.setFlinkJobId(jobContext.getJobId());
                            errorRecord.setDataSourceId(jdbcSinkConfig.getDbDatasourceId());
                            errorRecord.setDbSchema(jdbcSinkConfig.getDbSchema());
                            errorRecord.setTableName(jdbcSinkConfig.getTable());
                            errorRecord.setErrorData(JSON.toJSONString(jsonObject, JSONWriter.Feature.WriteMapNullValue, JSONWriter.Feature.WriteNullListAsEmpty));
                            errorRecord.setErrorMessage(ee.getMessage());
                            sqlErrorType.add(ee.getMessage());
                            try {
                                util.insertErrorData(errorRecord);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    default String modifyTimestamp(JdbcSinkConfig jdbcSinkConfig) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        String currentTimeString = now.format(formatter);
        String autoTimestampColumnName = jdbcSinkConfig.getPreConfig().getAutoTimestampColumnName();
        String sql = "update %s set " + autoTimestampColumnName + "='%s' ";
        if (StringUtils.isNoneBlank(jdbcSinkConfig.getDbSchema())) {
            sql = String.format(sql, jdbcSinkConfig.getDbSchema() + "." + jdbcSinkConfig.getTable(), currentTimeString);
        }
        else {
            sql = String.format(sql, jdbcSinkConfig.getTable(), currentTimeString);
        }
        return sql;
    }
}
