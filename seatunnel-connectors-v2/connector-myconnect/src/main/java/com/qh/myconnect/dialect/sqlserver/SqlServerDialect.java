package com.qh.myconnect.dialect.sqlserver;

import com.qh.myconnect.config.PreConfig;
import org.apache.commons.lang3.StringUtils;

import org.stringtemplate.v4.ST;

import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.converter.JdbcRowConverter;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
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

public class SqlServerDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "SqlServer";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new SqlServerJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new SqlserverTypeMapper();
    }

    public String copyTableOnlyColumn(
            String sourceTable, String targetTable, JdbcSinkConfig jdbcSinkConfig) {
        return format(
                "select  * into %s.%s from %s.%s where 1=2 ",
                jdbcSinkConfig.getDbSchema(),
                targetTable,
                jdbcSinkConfig.getDbSchema(),
                sourceTable);
    }

    public ResultSetMetaData getResultSetMetaData(Connection conn, JdbcSinkConfig jdbcSourceConfig)
            throws SQLException {
        String table = jdbcSourceConfig.getTable();
        Map<String, String> fieldMapper = jdbcSourceConfig.getFieldMapper();
        List<String> columns = new ArrayList<>();
        fieldMapper.forEach(
                (k, v) -> {
                    columns.add("[" + v + "]");
                });
        String sql =
                String.format(
                        "select  %s from %s.%s where 1=2 ",
                        StringUtils.join(columns, ","), jdbcSourceConfig.getDbSchema(), table);
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.executeQuery();
        return ps.getMetaData();
    }

    public String quoteIdentifier(String identifier) {
        return "[" + identifier + "]";
    }

    public String insertTableSql(
            JdbcSinkConfig jdbcSinkConfig, List<String> columns, List<String> values) {
        List<String> newColumns =
                columns.stream().map(x -> "\"" + x + "\"").collect(Collectors.toList());
        String sql =
                "insert into "
                + jdbcSinkConfig.getDbSchema()
                + "."
                + jdbcSinkConfig.getTable()
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
                + jdbcSinkConfig.getDbSchema()
                + "."
                + "XJ$_" + jdbcSinkConfig.getTable()
                + String.format("(%s)", StringUtils.join(newColumns, ","))
                + String.format("values (%s)", StringUtils.join(values, ","));
        return sql;
    }

    public String truncateTable(JdbcSinkConfig jdbcSinkConfig) {
        return String.format(
                "truncate  table %s.%s", jdbcSinkConfig.getDbSchema(), jdbcSinkConfig.getTable());
    }

    public String dropTable(JdbcSinkConfig jdbcSinkConfig, String tableName) {
        return String.format("drop table  %s.%s", jdbcSinkConfig.getDbSchema(), tableName);
    }

    public String createIndex(String tmpTableName, JdbcSinkConfig jdbcSinkConfig) {
        List<String> collect =
                jdbcSinkConfig.getPrimaryKeys().stream()
                        .map(x -> "\"" + x + "\"")
                        .collect(Collectors.toList());
        return String.format(
                "CREATE UNIQUE INDEX %s ON %s.%s(%s)",
                "inx_" + tmpTableName,
                jdbcSinkConfig.getDbSchema(),
                tmpTableName,
                StringUtils.join(collect, ','));
    }

    public int deleteData(
            Connection connection, String table, String ucTable, List<ColumnMapper> ucColumns) {
        String delSql =
                "delete from  <table>    "
                + " where not exists "
                + "       (select  <pks:{pk | <pk.sinkColumnName>}; separator=\" , \"> from <tmpTable> b where <pks:{pk | <table>.<pk.sinkColumnName>=b.<pk.sinkColumnName> }; separator=\" and \">  ) ";
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

    public String getSinkQueryUpdate(
            List<ColumnMapper> columnMappers, int rowSize, JdbcSinkConfig jdbcSinkConfig) {
        List<ColumnMapper> ucColumns =
                columnMappers.stream().filter(ColumnMapper::isUc).collect(Collectors.toList());
        String sqlQueryString =
                " select <columns:{sub | \"<sub.sinkColumnName>\" }; separator=\", \"> "
                + "  from <dbSchema>.<table> a "
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

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {

        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(uniqueKeyFields).contains(fieldName))
                        .collect(Collectors.toList());
        String valuesBinding =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));

        String usingClause = String.format("SELECT %s", valuesBinding);
        String onConditions =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(" AND "));
        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(", "));
        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertValues =
                Arrays.stream(fieldNames)
                        .map(fieldName -> "[SOURCE]." + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        "MERGE INTO %s.%s AS [TARGET]"
                        + " USING (%s) AS [SOURCE]"
                        + " ON (%s)"
                        + " WHEN MATCHED THEN"
                        + " UPDATE SET %s"
                        + " WHEN NOT MATCHED THEN"
                        + " INSERT (%s) VALUES (%s);",
                        database,
                        tableName,
                        usingClause,
                        onConditions,
                        updateSetClause,
                        insertFields,
                        insertValues);

        return Optional.of(upsertSQL);
    }
}
