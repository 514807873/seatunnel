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

package com.qh.myconnect.dialect.pgsql;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.qh.myconnect.config.PreConfig;
import org.apache.commons.lang3.StringUtils;

import org.stringtemplate.v4.ST;

import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.converter.JdbcRowConverter;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class PostgresDialect implements JdbcDialect {

    public static final int DEFAULT_POSTGRES_FETCH_SIZE = 128;

    @Override
    public String dialectName() {
        return "PostgreSQL";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new PostgresJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new PostgresTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String updateClause =
                Arrays.stream(fieldNames)
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                        + "=EXCLUDED."
                                        + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        "%s ON CONFLICT (%s) DO UPDATE SET %s",
                        getInsertIntoStatement(database, tableName, fieldNames),
                        uniqueColumns,
                        updateClause);
        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        // use cursor mode, reference:
        // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
        connection.setAutoCommit(false);
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        }
        else {
            statement.setFetchSize(DEFAULT_POSTGRES_FETCH_SIZE);
        }
        return statement;
    }

    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }


    public ResultSetMetaData getResultSetMetaData(Connection conn, JdbcSinkConfig jdbcSourceConfig)
            throws SQLException {
        String table = jdbcSourceConfig.getTable();
        Map<String, String> fieldMapper = jdbcSourceConfig.getFieldMapper();
        List<String> columns = new ArrayList<>();
        fieldMapper.forEach(
                (k, v) -> {
                    columns.add("\"" + v + "\"");
                });
        String sql =
                String.format(
                        "select  %s from \"%s\".\"%s\" where 1=2 ",
                        StringUtils.join(columns, ","), jdbcSourceConfig.getDbSchema(), table);
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.executeQuery();
        return ps.getMetaData();
    }

    public String getSinkQueryUpdate(
            List<ColumnMapper> columnMappers, int rowSize, JdbcSinkConfig jdbcSinkConfig) {
        List<ColumnMapper> ucColumns =
                columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString =
                " select <columns:{sub | \"<sub.sinkColumnName>\" }; separator=\", \"> "
                + "  from \"<dbSchema>\".\"<table>\" a "
                + " where  ";
        ST sqlQueryTemplate = new ST(sqlQueryString);
        sqlQueryTemplate.add("dbSchema", jdbcSinkConfig.getDbSchema());
        sqlQueryTemplate.add("table", jdbcSinkConfig.getTable());
        sqlQueryTemplate.add("columns", columnMappers);
        String sqlQuery = sqlQueryTemplate.render();
        List<String> where = new ArrayList<>();
        for (int i = 0; i < rowSize; i++) {
            String tmpWhere =
                    "( <ucs:{uc | \"<uc.sinkColumnName>\" = ?  }; separator=\" and \">  )";
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

    public String insertTableSql(
            JdbcSinkConfig jdbcSinkConfig, List<String> columns, List<String> values) {
        List<String> newColumns =
                columns.stream().map(x -> "\"" + x + "\"").collect(Collectors.toList());
        String sql =
                "insert into "
                + "\"" + jdbcSinkConfig.getDbSchema() + "\""
                + "."
                + "\"" + jdbcSinkConfig.getTable() + "\""
                + String.format("(%s)", StringUtils.join(newColumns, ","))
                + String.format("values (%s)", StringUtils.join(values, ","));
        return sql;
    }

    public String insertTmpTableSql(
            JdbcSinkConfig jdbcSinkConfig, List<String> columns, List<String> values) {
        List<String> newColumns =
                columns.stream().map(x -> "\"" + x + "\"").collect(Collectors.toList());
        String sql =
                "insert into "
                + "\"" + jdbcSinkConfig.getDbSchema() + "\""
                + "."
                + "\"" +"XJ$_" + jdbcSinkConfig.getTable() + "\""
                + String.format("(%s)", StringUtils.join(newColumns, ","))
                + String.format("values (%s)", StringUtils.join(values, ","));
        return sql;
    }

    public String truncateTable(JdbcSinkConfig jdbcSinkConfig) {
        return String.format(
                "truncate  table \"%s\".\"%s\"", jdbcSinkConfig.getDbSchema(), jdbcSinkConfig.getTable());
    }

    public String dropTable(JdbcSinkConfig jdbcSinkConfig, String tableName) {
        return String.format("drop table  \"%s\".\"%s\"", jdbcSinkConfig.getDbSchema(), tableName);
    }

    public String copyTableOnlyColumn(
            String sourceTable, String targetTable, JdbcSinkConfig jdbcSinkConfig) {
        List<String> collect =
                jdbcSinkConfig.getPrimaryKeys().stream()
                        .map(x -> "\"" + x + "\"")
                        .collect(Collectors.toList());
        return format(
                "create  table \"%s\".\"%s\" as select  * from \"%s\".\"%s\" where 1=2 ",
                jdbcSinkConfig.getDbSchema(),
                targetTable,
//                StringUtils.join(collect, ','),
                jdbcSinkConfig.getDbSchema(),
                sourceTable);
    }

    public int deleteData(
            Connection connection, String table, String ucTable, List<ColumnMapper> ucColumns) {
        String jsonString = JSON.toJSONString(ucColumns);
        List<ColumnMapper> newColumnMappers = JSON.parseArray(jsonString, ColumnMapper.class);
        newColumnMappers.forEach(
                x -> {
                    x.setSinkColumnName("\"" + x.getSinkColumnName() + "\"");
                });
        String delSql =
                "delete from  <table> a   "
                + " where not exists "
                + "       (select  <pks:{pk | <pk.sinkColumnName>}; separator=\" , \"> from <tmpTable> b where <pks:{pk | a.<pk.sinkColumnName>=b.<pk.sinkColumnName> }; separator=\" and \">  ) ";
        ST template = new ST(delSql);
        template.add("table", StringUtils.join(Arrays.stream(table.split("\\.")).map(x -> "\"" + x + "\"").collect(Collectors.toList()), "."));
        template.add("tmpTable", StringUtils.join(Arrays.stream(ucTable.split("\\.")).map(x -> "\"" + x + "\"").collect(Collectors.toList()), "."));
        template.add("pks", newColumnMappers);
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

    public int deleteDataLogic(
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
        String jsonString = JSON.toJSONString(ucColumns);
        List<ColumnMapper> newColumnMappers = JSON.parseArray(jsonString, ColumnMapper.class);
        newColumnMappers.forEach(
                x -> {
                    x.setSinkColumnName(this.quoteIdentifier(x.getSinkColumnName()));
                });
        String delSql =
                "update  <table> a   "
                + " set a.<operateColumnName> = 'D', a.<timestampColumnName> = '<timestampValue>' "
                + " where not exists "
                + "       (select  <pks:{pk | <pk.sinkColumnName>}; separator=\" , \"> from <tmpTable> b where <pks:{pk | a.<pk.sinkColumnName>=b.<pk.sinkColumnName> }; separator=\" and \">  ) "
                + " and <operateColumnName> !='D'";
        ST template = new ST(delSql);
        template.add("table", StringUtils.join(Arrays.stream(table.split("\\.")).map(x -> "\"" + x + "\"").collect(Collectors.toList()), "."));
        template.add("tmpTable", StringUtils.join(Arrays.stream(ucTable.split("\\.")).map(x -> "\"" + x + "\"").collect(Collectors.toList()), "."));
        template.add("pks", newColumnMappers);
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

    public Long getTableCount(Connection connection, String schema, String table) {
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

    public String createIndex(String tmpTableName, JdbcSinkConfig jdbcSinkConfig) {
        List<String> collect =
                jdbcSinkConfig.getPrimaryKeys().stream()
                        .map(x -> "\"" + x + "\"")
                        .collect(Collectors.toList());
        return String.format(
                "CREATE UNIQUE INDEX \"%s\" ON \"%s\".\"%s\"(%s)",
                "inx_" + tmpTableName,
                jdbcSinkConfig.getDbSchema(),
                tmpTableName,
                StringUtils.join(collect, ','));
    }
}
