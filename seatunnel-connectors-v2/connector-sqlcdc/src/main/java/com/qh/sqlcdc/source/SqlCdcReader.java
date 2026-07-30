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

import org.apache.seatunnel.shade.com.google.common.collect.Sets;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;

import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.config.Util;
import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class SqlCdcReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final SqlCdcConfig sqlCdcConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private List<SeaTunnelRow> localSeaTunnelRowsCache = new ArrayList<>();
    // 仅当开启删除操作时使用的缓存
    private Set<PrimaryKeyValue> localPrimaryKeysCacheForDelete = new HashSet<>();

    SqlCdcReader(SqlCdcConfig sqlCdcConfig, SeaTunnelRowType seaTunnelRowType) {
        this.sqlCdcConfig = sqlCdcConfig;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws IOException {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        Util util = new Util();
        List<SeaTunnelRow> nowSeaTunnelRows = new ArrayList<>();
        Set<PrimaryKeyValue> nowPrimaryKeysForDelete = new HashSet<>();
        try (Connection conn = util.getConnection(this.sqlCdcConfig)) {
            JdbcDialect jdbcDialect =
                    JdbcDialectFactory.getJdbcDialect(this.sqlCdcConfig.getDbType());
            try (PreparedStatement ps = conn.prepareStatement(this.sqlCdcConfig.getQuery())) {
                ps.executeQuery();
                ResultSet resultSet = ps.getResultSet();

                // 处理当前结果集
                while (resultSet.next()) {
                    SeaTunnelRow seaTunnelRow =
                            jdbcDialect
                                    .getRowConverter()
                                    .toInternal(resultSet, this.seaTunnelRowType);
                    nowSeaTunnelRows.add(seaTunnelRow);
                    // 如果开启了删除操作，收集主键信息
                    if (sqlCdcConfig.getOpenDelete()) {
                        PrimaryKeyValue pkValue = extractPrimaryKeyValue(seaTunnelRow);
                        if (pkValue != null) {
                            nowPrimaryKeysForDelete.add(pkValue);
                        }
                    }
                }

                // 保持原来的新增/更新检测逻辑不变
                Sets.SetView<SeaTunnelRow> newOrUpdated =
                        Sets.difference(
                                Sets.newHashSet(nowSeaTunnelRows),
                                Sets.newHashSet(localSeaTunnelRowsCache));
                for (SeaTunnelRow seaTunnelRow : newOrUpdated) {
                    output.collect(seaTunnelRow);
                }

                // 只有开启删除操作时才执行基于主键的删除检测
                if (sqlCdcConfig.getOpenDelete()) {
                    Set<PrimaryKeyValue> deletedKeys =
                            new HashSet<>(localPrimaryKeysCacheForDelete);
                    deletedKeys.removeAll(nowPrimaryKeysForDelete);
                    for (PrimaryKeyValue deletedKey : deletedKeys) {
                        SeaTunnelRow deletedRow =
                                findRowByPrimaryKey(localSeaTunnelRowsCache, deletedKey);
                        if (deletedRow != null) {
                            SeaTunnelRow deletedMarker = createDeleteMarker(deletedRow);
                            output.collect(deletedMarker);
                        }
                    }
                }

                // 更新缓存
                localSeaTunnelRowsCache.clear();
                for (SeaTunnelRow nowSeaTunnelRow : nowSeaTunnelRows) {
                    localSeaTunnelRowsCache.add(nowSeaTunnelRow.copy());
                }

                // 如果开启了删除操作，更新主键缓存
                if (sqlCdcConfig.getOpenDelete()) {
                    localPrimaryKeysCacheForDelete.clear();
                    localPrimaryKeysCacheForDelete.addAll(nowPrimaryKeysForDelete);
                }
            }
            Thread.sleep(1000 * 3);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 以下辅助方法保持不变
    private PrimaryKeyValue extractPrimaryKeyValue(SeaTunnelRow row) {
        if (sqlCdcConfig.getPrimaryKeys() == null || sqlCdcConfig.getPrimaryKeys().isEmpty()) {
            return null;
        }

        Object[] pkValues = new Object[sqlCdcConfig.getPrimaryKeys().size()];
        for (int i = 0; i < sqlCdcConfig.getPrimaryKeys().size(); i++) {
            String pkField = sqlCdcConfig.getPrimaryKeys().get(i);
            int fieldIndex = seaTunnelRowType.indexOf(pkField);
            if (fieldIndex == -1) {
                return null;
            }
            pkValues[i] = row.getField(fieldIndex);
        }
        return new PrimaryKeyValue(pkValues);
    }

    private SeaTunnelRow findRowByPrimaryKey(List<SeaTunnelRow> rows, PrimaryKeyValue pkValue) {
        for (SeaTunnelRow row : rows) {
            PrimaryKeyValue currentPk = extractPrimaryKeyValue(row);
            if (currentPk != null && currentPk.equals(pkValue)) {
                return row;
            }
        }
        return null;
    }

    private SeaTunnelRow createDeleteMarker(SeaTunnelRow originalRow) {
        Object[] fields = new Object[originalRow.getFields().length];
        System.arraycopy(originalRow.getFields(), 0, fields, 0, fields.length);
        SeaTunnelRow marker = new SeaTunnelRow(fields);
        marker.setTableId("default.default.cdc_table");
        marker.setRowKind(RowKind.DELETE);
        return marker;
    }

    private static class PrimaryKeyValue {
        private final Object[] values;

        PrimaryKeyValue(Object[] values) {
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKeyValue that = (PrimaryKeyValue) o;
            return Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
