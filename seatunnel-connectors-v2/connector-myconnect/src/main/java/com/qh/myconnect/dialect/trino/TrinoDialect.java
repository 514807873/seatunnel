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

package com.qh.myconnect.dialect.trino;


import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.config.Util;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.converter.JdbcRowConverter;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.stringtemplate.v4.ST;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TrinoDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "Trino";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new TrinoJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new TrinoTypeMapper();
    }


    public String truncateTable(JdbcSinkConfig jdbcSinkConfig) {
        return String.format(
                "delete from  %s.%s", jdbcSinkConfig.getDbSchema(), jdbcSinkConfig.getTable());
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
                        "select  %s from %s.%s where 1=2 ",
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
        return Optional.empty();
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
                + String.format("(%s)", StringUtils.join(newColumns, ","));
        List<String> valuesArray = new ArrayList<>();
        for (int i = 0; i < jdbcSinkConfig.getBatchSize(); i++) {
            valuesArray.add(String.format("(%s)", StringUtils.join(values, ",")));
        }
        sql += String.format(" values %s", StringUtils.join(valuesArray, ","));
        return sql;
    }

    public void insertToDb(List<ColumnMapper> columnMappers,
                           JdbcSinkConfig jdbcSinkConfig,
                           Connection conn,
                           Map<String, String> metaDataHash,
                           List<SeaTunnelRow> seaTunnelRows,
                           Util util,
                           JobContext jobContext,
                           Set<String> sqlErrorType,
                           Long insertCount,
                           Long errorCount
    ) {
        Long tmpInsertCount = null;
        String sql = null;
        try {
            List<String> columns = columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
            List<String> values = columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            if (jdbcSinkConfig.getBatchSize() != seaTunnelRows.size()) {
                jdbcSinkConfig.setBatchSize(seaTunnelRows.size());
            }
            sql = this.insertTableSql(jdbcSinkConfig, columns, values);
            PreparedStatement psUpsert = conn.prepareStatement(sql);
            tmpInsertCount = insertCount;
            for (int i = 0; i < seaTunnelRows.size(); i++) {
                SeaTunnelRow seaTunnelRow = seaTunnelRows.get(i);
                if (seaTunnelRow != null) {
                    for (int j = 0; j < columnMappers.size(); j++) {
                        Integer valueIndex = columnMappers.get(j).getSourceRowPosition();
                        Object field =
                                columnMappers.get(j).getConverter().apply(seaTunnelRow.getField(valueIndex));
                        String column = columns.get(j);
                        String dbType = metaDataHash.get(column);
                        this.setPreparedStatementValueByDbType(j + (columnMappers.size() * i) + 1,
                                psUpsert, dbType,
                                util.Object2String(field));
                    }
                }
                insertCount++;
            }
            psUpsert.addBatch();
            psUpsert.executeBatch();
            psUpsert.clearBatch();
            psUpsert.close();
        } catch (Exception e) {
            log.error("错误sql:" + sql, e);
            insertCount = tmpInsertCount;
            throw new RuntimeException(e);

        }
    }

}
