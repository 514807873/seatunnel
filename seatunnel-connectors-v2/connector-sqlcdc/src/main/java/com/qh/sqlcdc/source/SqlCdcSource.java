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

package com.qh.sqlcdc.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.google.auto.service.AutoService;
import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.config.Util;
import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectFactory;
import com.qh.sqlcdc.dialect.JdbcDialectTypeMapper;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(SeaTunnelSource.class)
@Slf4j
public class SqlCdcSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportCoordinate {
    private SqlCdcConfig sqlCdcConfig;
    private JobContext jobContext;
    private JdbcDialect jdbcDialect;

    @Override
    public String getPluginName() {
        return "SqlCdc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.sqlCdcConfig = new SqlCdcConfig(pluginConfig);
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(sqlCdcConfig.getDbType());
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return initTableField();
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        SeaTunnelRowType rowType = initTableField();
        // 1. 构建列信息
        List<Column> columns = new ArrayList<>();
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            Integer scale = null;
            Long columnLength = null;
            PhysicalColumn column =
                    PhysicalColumn.of(
                            rowType.getFieldName(i),
                            rowType.getFieldType(i),
                            columnLength,
                            scale,
                            true,
                            null,
                            null);
            schemaBuilder.column(column);
        }

        // 2. 构建主键（如果配置了）
        PrimaryKey primaryKey = null;
        if (sqlCdcConfig.getPrimaryKeys() != null && !sqlCdcConfig.getPrimaryKeys().isEmpty()) {
            primaryKey =
                    PrimaryKey.of(
                            "pk_" + sqlCdcConfig.getPrimaryKeys(), sqlCdcConfig.getPrimaryKeys());
        }
        // 3. 构建 TableSchema
        TableSchema tableSchema = schemaBuilder.primaryKey(primaryKey).build();
        Map<String, String> options = new HashMap<>();
        options.put("changelog-mode", "I,U,D");
        // 4. 创建 CatalogTable
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "cdc_table"),
                        tableSchema,
                        options,
                        Collections.emptyList(),
                        null);

        return Collections.singletonList(catalogTable);
    }

    private SeaTunnelRowType initTableField() {
        Util util = new Util();
        Connection conn = util.getConnection(this.sqlCdcConfig);
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            PreparedStatement ps = conn.prepareStatement(sqlCdcConfig.getQuery());
            ps.executeQuery();
            ResultSetMetaData resultSetMetaData = ps.getMetaData();
            JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnLabel(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
            ps.close();
            conn.close();
        } catch (Exception e) {
            log.warn("get row type info exception", e);
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new SqlCdcReader(this.sqlCdcConfig, this.initTableField());
    }
}
