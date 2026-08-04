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

import com.google.common.collect.Sets;
import org.apache.seatunnel.api.source.Collector;

import com.qh.sqlcdc.compare.CanonicalNormalizer;
import com.qh.sqlcdc.compare.NumericPkComparator;
import com.qh.sqlcdc.compare.NumericTypeDetector;
import com.qh.sqlcdc.compare.OrderedQueryBuilder;
import com.qh.sqlcdc.config.DirectSinkConfig;
import com.qh.sqlcdc.config.SqlCdcConfig;
import com.qh.sqlcdc.config.Util;
import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class SqlCdcReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final SqlCdcConfig sqlCdcConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private final Util util = new Util();
    /** 无目标配置时回退本地缓存对比 */
    private List<SeaTunnelRow> localSeaTunnelRowsCache = new ArrayList<>();
    private Set<PrimaryKeyValue> localPrimaryKeysCacheForDelete = new HashSet<>();
    /** 对账差异待下发队列；pending 未空时只分批 collect，不重新对账 */
    private final Deque<SeaTunnelRow> pendingEmitRows = new ArrayDeque<>();

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
        try {
            if (!pendingEmitRows.isEmpty()) {
                emitBatch(output);
                return;
            }

            if (sqlCdcConfig.hasDirectSinkConfigs()) {
                fillPendingFromTargetCompare();
            } else {
                fillPendingFromLocalCache(loadSourceRows(sqlCdcConfig.getQuery()));
            }
            emitBatch(output);

            if (pendingEmitRows.isEmpty()) {
                Thread.sleep(1000 * 3);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<SeaTunnelRow> loadSourceRows(String query) throws Exception {
        List<SeaTunnelRow> nowSeaTunnelRows = new ArrayList<>();
        try (Connection conn = util.getConnection(this.sqlCdcConfig)) {
            JdbcDialect jdbcDialect =
                    JdbcDialectFactory.getJdbcDialect(this.sqlCdcConfig.getDbType());
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.executeQuery();
                ResultSet resultSet = ps.getResultSet();
                while (resultSet.next()) {
                    SeaTunnelRow seaTunnelRow =
                            jdbcDialect
                                    .getRowConverter()
                                    .toInternal(resultSet, this.seaTunnelRowType);
                    nowSeaTunnelRows.add(seaTunnelRow);
                }
            }
        }
        return nowSeaTunnelRows;
    }

    private void emitBatch(Collector<SeaTunnelRow> output) {
        int batchSize = sqlCdcConfig.getEmitBatchSize();
        int emitted = 0;
        while (emitted < batchSize && !pendingEmitRows.isEmpty()) {
            output.collect(pendingEmitRows.pollFirst());
            emitted++;
        }
        log.info(
                "SqlCdc 分批吐数: emitted={}, remaining={}, batchSize={}",
                emitted,
                pendingEmitRows.size(),
                batchSize);
    }

    /**
     * 目标对账：数字主键走双游标；字符/混合主键走 Map&lt;pk,fp&gt;。
     * insert / update / delete 分别打 RowKind，供 JDBC Sink 写入 streamRecord。
     */
    private void fillPendingFromTargetCompare() throws Exception {
        Map<PrimaryKeyValue, SeaTunnelRow> insertRows = new LinkedHashMap<>();
        Map<PrimaryKeyValue, SeaTunnelRow> updateRows = new LinkedHashMap<>();
        Map<PrimaryKeyValue, SeaTunnelRow> deleteRows = new LinkedHashMap<>();
        List<SeaTunnelRow> sourceRowsForMap = null;

        for (DirectSinkConfig sinkConfig : sqlCdcConfig.getDirectSinkConfigs()) {
            if (sinkConfig.getPrimaryKeys() == null || sinkConfig.getPrimaryKeys().isEmpty()) {
                throw new RuntimeException("directSinkConfigs.primary_keys 不能为空，无法与目标对账");
            }
            if (sinkConfig.getFieldMapper() == null || sinkConfig.getFieldMapper().isEmpty()) {
                throw new RuntimeException("directSinkConfigs.field_mapper 不能为空，无法与目标对账");
            }

            List<String> targetPks = sinkConfig.getPrimaryKeys();
            List<String> sourcePks = new ArrayList<>();
            for (String targetPk : targetPks) {
                String sourcePk = reverseMapField(targetPk, sinkConfig.getFieldMapper());
                if (sourcePk == null) {
                    throw new RuntimeException(
                            "目标主键[" + targetPk + "] 在 field_mapper 中找不到对应源字段");
                }
                sourcePks.add(sourcePk);
            }

            boolean sourceNumeric =
                    NumericTypeDetector.allSourcePkNumeric(seaTunnelRowType, sourcePks);
            if (sourceNumeric) {
                log.info(
                        "SqlCdc 主键判定为数字，尝试双游标对账: sourcePks={}, targetPks={}",
                        sourcePks,
                        targetPks);
                boolean usedDual =
                        compareByDualCursor(
                                sinkConfig,
                                sourcePks,
                                targetPks,
                                insertRows,
                                updateRows,
                                deleteRows);
                if (usedDual) {
                    continue;
                }
                log.warn("SqlCdc 目标主键非数字或双游标失败，回退 Map 指纹对账");
            } else {
                log.info(
                        "SqlCdc 主键含非数字类型，使用 Map 指纹对账: sourcePks={}",
                        sourcePks);
            }

            if (sourceRowsForMap == null) {
                sourceRowsForMap = loadSourceRows(sqlCdcConfig.getQuery());
            }
            compareByFingerprintMap(
                    sinkConfig,
                    sourcePks,
                    targetPks,
                    sourceRowsForMap,
                    insertRows,
                    updateRows,
                    deleteRows);
        }

        pendingEmitRows.addAll(insertRows.values());
        pendingEmitRows.addAll(updateRows.values());
        pendingEmitRows.addAll(deleteRows.values());
        log.info(
                "SqlCdc 目标对账完成: insert={}, update={}, delete={}, pending={}",
                insertRows.size(),
                updateRows.size(),
                deleteRows.size(),
                pendingEmitRows.size());
    }

    /**
     * 双游标归并。成功返回 true；目标主键非数字时返回 false 以便回退 Map。
     */
    private boolean compareByDualCursor(
            DirectSinkConfig sinkConfig,
            List<String> sourcePks,
            List<String> targetPks,
            Map<PrimaryKeyValue, SeaTunnelRow> insertRows,
            Map<PrimaryKeyValue, SeaTunnelRow> updateRows,
            Map<PrimaryKeyValue, SeaTunnelRow> deleteRows)
            throws Exception {
        String sourceOrdered =
                OrderedQueryBuilder.wrapOrderBy(
                        sqlCdcConfig.getQuery(), sourcePks, sqlCdcConfig.getDbType());
        String targetOrdered =
                OrderedQueryBuilder.wrapOrderBy(
                        sinkConfig.getQuery(), targetPks, sinkConfig.getDbType());

        JdbcDialect sourceDialect =
                JdbcDialectFactory.getJdbcDialect(this.sqlCdcConfig.getDbType());
        int insert = 0;
        int update = 0;
        int delete = 0;
        int unchanged = 0;

        try (Connection sourceConn = util.getConnection(sqlCdcConfig);
                Connection targetConn = util.getConnection(sinkConfig);
                PreparedStatement sourcePs = sourceConn.prepareStatement(sourceOrdered);
                PreparedStatement targetPs = targetConn.prepareStatement(targetOrdered)) {
            sourcePs.executeQuery();
            targetPs.executeQuery();
            ResultSet sourceRs = sourcePs.getResultSet();
            ResultSet targetRs = targetPs.getResultSet();

            ResultSetMetaData targetMeta = targetRs.getMetaData();
            if (!NumericTypeDetector.allColumnsNumeric(targetMeta, targetPks)) {
                return false;
            }

            List<String> targetLabels = new ArrayList<>();
            for (int i = 1; i <= targetMeta.getColumnCount(); i++) {
                targetLabels.add(targetMeta.getColumnLabel(i));
            }

            boolean sourceHasNext = sourceRs.next();
            boolean targetHasNext = targetRs.next();
            SeaTunnelRow sourceRow = null;
            PrimaryKeyValue sourcePk = null;
            String sourceFp = null;
            Map<String, Object> targetRow = null;
            PrimaryKeyValue targetPk = null;
            String targetFp = null;

            if (sourceHasNext) {
                sourceRow = sourceDialect.getRowConverter().toInternal(sourceRs, seaTunnelRowType);
                sourcePk = extractPrimaryKeyValue(sourceRow, sourcePks);
                sourceFp = fingerprintFromSource(sourceRow, sinkConfig.getFieldMapper());
            }
            if (targetHasNext) {
                targetRow = readTargetRow(targetRs, targetLabels);
                targetPk = extractPrimaryKeyValueFromMap(targetRow, targetPks);
                targetFp = fingerprintFromTarget(targetRow, sinkConfig.getFieldMapper());
            }

            while (sourceHasNext || targetHasNext) {
                if (sourceHasNext && sourcePk == null) {
                    sourceHasNext = sourceRs.next();
                    if (sourceHasNext) {
                        sourceRow =
                                sourceDialect
                                        .getRowConverter()
                                        .toInternal(sourceRs, seaTunnelRowType);
                        sourcePk = extractPrimaryKeyValue(sourceRow, sourcePks);
                        sourceFp = fingerprintFromSource(sourceRow, sinkConfig.getFieldMapper());
                    }
                    continue;
                }
                if (targetHasNext && targetPk == null) {
                    targetHasNext = targetRs.next();
                    if (targetHasNext) {
                        targetRow = readTargetRow(targetRs, targetLabels);
                        targetPk = extractPrimaryKeyValueFromMap(targetRow, targetPks);
                        targetFp = fingerprintFromTarget(targetRow, sinkConfig.getFieldMapper());
                    }
                    continue;
                }

                if (sourceHasNext && targetHasNext) {
                    int cmp =
                            NumericPkComparator.compare(sourcePk.getValues(), targetPk.getValues());
                    if (cmp == 0) {
                        if (!sourceFp.equals(targetFp)) {
                            updateRows.put(
                                    sourcePk, markRowKind(sourceRow, RowKind.UPDATE_AFTER));
                            update++;
                        } else {
                            unchanged++;
                        }
                        sourceHasNext = sourceRs.next();
                        targetHasNext = targetRs.next();
                        if (sourceHasNext) {
                            sourceRow =
                                    sourceDialect
                                            .getRowConverter()
                                            .toInternal(sourceRs, seaTunnelRowType);
                            sourcePk = extractPrimaryKeyValue(sourceRow, sourcePks);
                            sourceFp =
                                    fingerprintFromSource(sourceRow, sinkConfig.getFieldMapper());
                        }
                        if (targetHasNext) {
                            targetRow = readTargetRow(targetRs, targetLabels);
                            targetPk = extractPrimaryKeyValueFromMap(targetRow, targetPks);
                            targetFp = fingerprintFromTarget(targetRow, sinkConfig.getFieldMapper());
                        }
                    } else if (cmp < 0) {
                        insertRows.put(sourcePk, markRowKind(sourceRow, RowKind.INSERT));
                        insert++;
                        sourceHasNext = sourceRs.next();
                        if (sourceHasNext) {
                            sourceRow =
                                    sourceDialect
                                            .getRowConverter()
                                            .toInternal(sourceRs, seaTunnelRowType);
                            sourcePk = extractPrimaryKeyValue(sourceRow, sourcePks);
                            sourceFp =
                                    fingerprintFromSource(sourceRow, sinkConfig.getFieldMapper());
                        }
                    } else {
                        if (Boolean.TRUE.equals(sqlCdcConfig.getOpenDelete())) {
                            deleteRows.put(targetPk, createDeleteMarkerFromPk(targetPk, sourcePks));
                            delete++;
                        }
                        targetHasNext = targetRs.next();
                        if (targetHasNext) {
                            targetRow = readTargetRow(targetRs, targetLabels);
                            targetPk = extractPrimaryKeyValueFromMap(targetRow, targetPks);
                            targetFp = fingerprintFromTarget(targetRow, sinkConfig.getFieldMapper());
                        }
                    }
                } else if (sourceHasNext) {
                    insertRows.put(sourcePk, markRowKind(sourceRow, RowKind.INSERT));
                    insert++;
                    sourceHasNext = sourceRs.next();
                    if (sourceHasNext) {
                        sourceRow =
                                sourceDialect
                                        .getRowConverter()
                                        .toInternal(sourceRs, seaTunnelRowType);
                        sourcePk = extractPrimaryKeyValue(sourceRow, sourcePks);
                        sourceFp = fingerprintFromSource(sourceRow, sinkConfig.getFieldMapper());
                    }
                } else {
                    if (Boolean.TRUE.equals(sqlCdcConfig.getOpenDelete())) {
                        deleteRows.put(targetPk, createDeleteMarkerFromPk(targetPk, sourcePks));
                        delete++;
                    }
                    targetHasNext = targetRs.next();
                    if (targetHasNext) {
                        targetRow = readTargetRow(targetRs, targetLabels);
                        targetPk = extractPrimaryKeyValueFromMap(targetRow, targetPks);
                        targetFp = fingerprintFromTarget(targetRow, sinkConfig.getFieldMapper());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "双游标对账失败, sourceOrderSql="
                            + sourceOrdered
                            + ", targetOrderSql="
                            + targetOrdered
                            + ", cause="
                            + e.getMessage(),
                    e);
        }

        log.info(
                "SqlCdc 双游标对账完成: insert={}, update={}, delete={}, unchanged={}",
                insert,
                update,
                delete,
                unchanged);
        return true;
    }

    /**
     * Map 指纹对账：先建目标 Map，扫源查 Map（命中 remove），剩余为删。
     */
    private void compareByFingerprintMap(
            DirectSinkConfig sinkConfig,
            List<String> sourcePks,
            List<String> targetPks,
            List<SeaTunnelRow> sourceRows,
            Map<PrimaryKeyValue, SeaTunnelRow> insertRows,
            Map<PrimaryKeyValue, SeaTunnelRow> updateRows,
            Map<PrimaryKeyValue, SeaTunnelRow> deleteRows)
            throws Exception {
        Map<PrimaryKeyValue, String> targetFingerprints =
                loadTargetFingerprints(sinkConfig, targetPks);
        int insert = 0;
        int update = 0;

        for (SeaTunnelRow sourceRow : sourceRows) {
            PrimaryKeyValue pk = extractPrimaryKeyValue(sourceRow, sourcePks);
            if (pk == null) {
                continue;
            }
            String sourceFp = fingerprintFromSource(sourceRow, sinkConfig.getFieldMapper());
            String targetFp = targetFingerprints.remove(pk);
            if (targetFp == null) {
                insertRows.put(pk, markRowKind(sourceRow, RowKind.INSERT));
                insert++;
            } else if (!sourceFp.equals(targetFp)) {
                updateRows.put(pk, markRowKind(sourceRow, RowKind.UPDATE_AFTER));
                update++;
            }
        }

        if (Boolean.TRUE.equals(sqlCdcConfig.getOpenDelete())) {
            for (PrimaryKeyValue remainPk : targetFingerprints.keySet()) {
                deleteRows.put(remainPk, createDeleteMarkerFromPk(remainPk, sourcePks));
            }
        }

        log.info(
                "SqlCdc Map指纹对账完成: sourceRows={}, insert={}, update={}, delete={}, targetRemain={}",
                sourceRows.size(),
                insert,
                update,
                deleteRows.size(),
                targetFingerprints.size());
    }

    private SeaTunnelRow markRowKind(SeaTunnelRow row, RowKind rowKind) {
        row.setRowKind(rowKind);
        return row;
    }

    private Map<String, Object> readTargetRow(ResultSet rs, List<String> labels) throws Exception {
        Map<String, Object> row = new HashMap<>();
        for (String label : labels) {
            row.put(label, rs.getObject(label));
        }
        return row;
    }

    /** 目标侧只落 pk→MD5，不缓存整行。 */
    private Map<PrimaryKeyValue, String> loadTargetFingerprints(
            DirectSinkConfig sinkConfig, List<String> targetPks) throws Exception {
        Map<PrimaryKeyValue, String> fingerprints = new LinkedHashMap<>();
        try (Connection conn = util.getConnection(sinkConfig);
                PreparedStatement ps = conn.prepareStatement(sinkConfig.getQuery())) {
            ps.executeQuery();
            ResultSet rs = ps.getResultSet();
            ResultSetMetaData metaData = rs.getMetaData();
            List<String> labels = new ArrayList<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                labels.add(metaData.getColumnLabel(i));
            }
            Map<String, String> fieldMapper = sinkConfig.getFieldMapper();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (String label : labels) {
                    row.put(label, rs.getObject(label));
                }
                PrimaryKeyValue pk = extractPrimaryKeyValueFromMap(row, targetPks);
                if (pk != null) {
                    fingerprints.put(pk, fingerprintFromTarget(row, fieldMapper));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "查询目标表失败, query=" + sinkConfig.getQuery() + ", cause=" + e.getMessage(), e);
        }
        return fingerprints;
    }

    private String fingerprintFromSource(SeaTunnelRow sourceRow, Map<String, String> fieldMapper) {
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : fieldMapper.entrySet()) {
            int idx = indexOfIgnoreCase(entry.getKey());
            values.put(entry.getKey(), idx >= 0 ? sourceRow.getField(idx) : null);
        }
        return CanonicalNormalizer.md5Fingerprint(values);
    }

    private String fingerprintFromTarget(Map<String, Object> targetRow, Map<String, String> fieldMapper) {
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : fieldMapper.entrySet()) {
            values.put(entry.getKey(), getMapValueIgnoreCase(targetRow, entry.getValue()));
        }
        return CanonicalNormalizer.md5Fingerprint(values);
    }

    private SeaTunnelRow createDeleteMarkerFromPk(PrimaryKeyValue pk, List<String> sourcePks) {
        Object[] fields = new Object[seaTunnelRowType.getTotalFields()];
        Object[] pkValues = pk.getValues();
        for (int i = 0; i < sourcePks.size() && i < pkValues.length; i++) {
            int fieldIndex = indexOfIgnoreCase(sourcePks.get(i));
            if (fieldIndex >= 0) {
                // 主键在对账时已归一化为 String，写出前按源 schema 还原类型
                fields[fieldIndex] =
                        coerceToSeaTunnelType(
                                pkValues[i], seaTunnelRowType.getFieldType(fieldIndex));
            }
        }
        SeaTunnelRow marker = new SeaTunnelRow(fields);
        marker.setTableId("default.default.cdc_table");
        marker.setRowKind(RowKind.DELETE);
        return marker;
    }

    private Object coerceToSeaTunnelType(Object value, SeaTunnelDataType<?> dataType) {
        if (value == null || dataType == null) {
            return value;
        }
        Class<?> expected = dataType.getTypeClass();
        if (expected.isInstance(value)) {
            return value;
        }
        String text = value instanceof byte[] ? null : String.valueOf(value).trim();
        SqlType sqlType = dataType.getSqlType();
        try {
            switch (sqlType) {
                case TINYINT:
                    return text == null ? value : Byte.valueOf(text);
                case SMALLINT:
                    return text == null ? value : Short.valueOf(text);
                case INT:
                    return text == null ? value : Integer.valueOf(text);
                case BIGINT:
                    return text == null ? value : Long.valueOf(text);
                case FLOAT:
                    return text == null ? value : Float.valueOf(text);
                case DOUBLE:
                    return text == null ? value : Double.valueOf(text);
                case DECIMAL:
                    return text == null
                            ? value
                            : (value instanceof BigDecimal
                                    ? value
                                    : new BigDecimal(text));
                case BOOLEAN:
                    if (value instanceof Boolean) {
                        return value;
                    }
                    return "1".equals(text)
                            || "true".equalsIgnoreCase(text)
                            || "yes".equalsIgnoreCase(text);
                case STRING:
                    return text == null ? String.valueOf(value) : text;
                case BYTES:
                    return value instanceof byte[] ? value : text.getBytes();
                default:
                    return value;
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "删除标记主键类型转换失败: value="
                            + value
                            + ", sqlType="
                            + sqlType
                            + ", expected="
                            + expected.getName(),
                    e);
        }
    }

    private void fillPendingFromLocalCache(List<SeaTunnelRow> nowSeaTunnelRows) {
        Set<PrimaryKeyValue> nowPrimaryKeysForDelete = new HashSet<>();
        if (Boolean.TRUE.equals(sqlCdcConfig.getOpenDelete())) {
            for (SeaTunnelRow seaTunnelRow : nowSeaTunnelRows) {
                PrimaryKeyValue pkValue =
                        extractPrimaryKeyValue(seaTunnelRow, sqlCdcConfig.getPrimaryKeys());
                if (pkValue != null) {
                    nowPrimaryKeysForDelete.add(pkValue);
                }
            }
        }

        Sets.SetView<SeaTunnelRow> newOrUpdated =
                Sets.difference(
                        Sets.newHashSet(nowSeaTunnelRows),
                        Sets.newHashSet(localSeaTunnelRowsCache));
        for (SeaTunnelRow seaTunnelRow : newOrUpdated) {
            pendingEmitRows.addLast(seaTunnelRow);
        }

        if (Boolean.TRUE.equals(sqlCdcConfig.getOpenDelete())) {
            Set<PrimaryKeyValue> deletedKeys = new HashSet<>(localPrimaryKeysCacheForDelete);
            deletedKeys.removeAll(nowPrimaryKeysForDelete);
            for (PrimaryKeyValue deletedKey : deletedKeys) {
                SeaTunnelRow deletedRow = findRowByPrimaryKey(localSeaTunnelRowsCache, deletedKey);
                if (deletedRow != null) {
                    pendingEmitRows.addLast(createDeleteMarker(deletedRow));
                }
            }
        }

        localSeaTunnelRowsCache.clear();
        for (SeaTunnelRow nowSeaTunnelRow : nowSeaTunnelRows) {
            localSeaTunnelRowsCache.add(nowSeaTunnelRow.copy());
        }
        if (Boolean.TRUE.equals(sqlCdcConfig.getOpenDelete())) {
            localPrimaryKeysCacheForDelete.clear();
            localPrimaryKeysCacheForDelete.addAll(nowPrimaryKeysForDelete);
        }
        log.info(
                "SqlCdc 本地缓存对账完成: sourceRows={}, pending={}",
                nowSeaTunnelRows.size(),
                pendingEmitRows.size());
    }

    private PrimaryKeyValue extractPrimaryKeyValue(SeaTunnelRow row, List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return null;
        }
        Object[] pkValues = new Object[primaryKeys.size()];
        for (int i = 0; i < primaryKeys.size(); i++) {
            int fieldIndex = indexOfIgnoreCase(primaryKeys.get(i));
            if (fieldIndex == -1) {
                return null;
            }
            Object normalized = CanonicalNormalizer.normalize(row.getField(fieldIndex));
            if (normalized == null) {
                return null;
            }
            pkValues[i] = normalized;
        }
        return new PrimaryKeyValue(pkValues);
    }

    private PrimaryKeyValue extractPrimaryKeyValueFromMap(
            Map<String, Object> row, List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return null;
        }
        Object[] pkValues = new Object[primaryKeys.size()];
        for (int i = 0; i < primaryKeys.size(); i++) {
            Object value = getMapValueIgnoreCase(row, primaryKeys.get(i));
            Object normalized = CanonicalNormalizer.normalize(value);
            if (normalized == null) {
                return null;
            }
            pkValues[i] = normalized;
        }
        return new PrimaryKeyValue(pkValues);
    }

    private SeaTunnelRow findRowByPrimaryKey(List<SeaTunnelRow> rows, PrimaryKeyValue pkValue) {
        for (SeaTunnelRow row : rows) {
            PrimaryKeyValue currentPk =
                    extractPrimaryKeyValue(row, sqlCdcConfig.getPrimaryKeys());
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

    private int indexOfIgnoreCase(String fieldName) {
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            if (seaTunnelRowType.getFieldName(i).equalsIgnoreCase(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    private String reverseMapField(String targetField, Map<String, String> fieldMapper) {
        for (Map.Entry<String, String> entry : fieldMapper.entrySet()) {
            if (entry.getValue() != null && entry.getValue().equalsIgnoreCase(targetField)) {
                return entry.getKey();
            }
        }
        if (getMapperValueIgnoreCase(fieldMapper, targetField) != null) {
            return targetField;
        }
        return null;
    }

    private String getMapperValueIgnoreCase(Map<String, String> fieldMapper, String sourceField) {
        if (fieldMapper.containsKey(sourceField)) {
            return fieldMapper.get(sourceField);
        }
        for (Map.Entry<String, String> entry : fieldMapper.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(sourceField)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private Object getMapValueIgnoreCase(Map<String, Object> row, String key) {
        if (row.containsKey(key)) {
            return row.get(key);
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static class PrimaryKeyValue {
        private final Object[] values;

        PrimaryKeyValue(Object[] values) {
            this.values = values;
        }

        Object[] getValues() {
            return values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PrimaryKeyValue that = (PrimaryKeyValue) o;
            return Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
