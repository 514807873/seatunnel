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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;

import org.apache.commons.collections4.CollectionUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utilities for pangu-style field/value mapping on JDBC sink rows. */
final class JdbcFieldMappingUtils {

    private JdbcFieldMappingUtils() {}

    static boolean hasFieldMapping(JdbcSinkConfig config) {
        return (config.getValueMapper() != null && !config.getValueMapper().isEmpty())
                || (config.getFieldMapper() != null && !config.getFieldMapper().isEmpty());
    }

    /**
     * Rebuild write schema with sink column names from value_mapper/field_mapper.
     *
     * <p>Upstream catalog schema keeps source names (ID/NAME), while primary_keys / SQL generation
     * use sink names (XH/XM). Without this remapping, upsert executor fails with "can't find field
     * [XH]".
     */
    static TableSchema rebuildSinkTableSchema(
            JdbcSinkConfig config, TableSchema upstreamSchema, TableSchema databaseTableSchema) {
        if (!hasFieldMapping(config)) {
            return upstreamSchema;
        }

        Map<String, String> valueMapper = effectiveValueMapper(config, upstreamSchema);
        List<Column> sinkColumns = new ArrayList<>();
        SeaTunnelRowType upstreamRowType = upstreamSchema.toPhysicalRowDataType();

        if (!valueMapper.isEmpty()) {
            List<Map.Entry<String, String>> ordered =
                    valueMapper.entrySet().stream()
                            .sorted(
                                    Comparator.comparingInt(
                                            e -> {
                                                try {
                                                    return Integer.parseInt(e.getKey());
                                                } catch (NumberFormatException ex) {
                                                    return Integer.MAX_VALUE;
                                                }
                                            }))
                            .collect(Collectors.toList());
            for (Map.Entry<String, String> entry : ordered) {
                int sourceIndex;
                try {
                    sourceIndex = Integer.parseInt(entry.getKey());
                } catch (NumberFormatException e) {
                    continue;
                }
                if (sourceIndex < 0 || sourceIndex >= upstreamSchema.getColumns().size()) {
                    continue;
                }
                Column sourceColumn = upstreamSchema.getColumns().get(sourceIndex);
                sinkColumns.add(sourceColumn.rename(entry.getValue()));
            }
        }

        if (sinkColumns.isEmpty() && config.getFieldMapper() != null) {
            for (Map.Entry<String, String> entry : config.getFieldMapper().entrySet()) {
                int sourceIndex = upstreamRowType.indexOf(entry.getKey(), false);
                if (sourceIndex < 0) {
                    continue;
                }
                sinkColumns.add(upstreamSchema.getColumns().get(sourceIndex).rename(entry.getValue()));
            }
        }

        if (Boolean.TRUE.equals(config.getRecordOperation())) {
            appendRecordOperationColumns(sinkColumns, databaseTableSchema);
        }

        if (sinkColumns.isEmpty()) {
            return upstreamSchema;
        }

        TableSchema.Builder builder = TableSchema.builder().columns(sinkColumns);
        List<String> pkNames = resolvePrimaryKeyNames(config, upstreamSchema);
        if (!pkNames.isEmpty()) {
            List<String> matched =
                    pkNames.stream()
                            .filter(
                                    pk ->
                                            sinkColumns.stream()
                                                    .anyMatch(c -> c.getName().equalsIgnoreCase(pk)))
                            .collect(Collectors.toList());
            if (!matched.isEmpty()) {
                builder.primaryKey(PrimaryKey.of(matched.get(0) + "_pk", matched));
            }
        }
        return builder.build();
    }

    static Integer resolvePrimaryKeyIndex(JdbcSinkConfig config, TableSchema tableSchema) {
        Optional<String> primaryKeyName = resolvePrimaryKeyName(config, tableSchema);
        if (!primaryKeyName.isPresent()) {
            return null;
        }
        return resolveSourceFieldIndex(config, tableSchema, primaryKeyName.get());
    }

    static SeaTunnelRow remapRow(
            SeaTunnelRow element, JdbcSinkConfig config, TableSchema tableSchema) {
        return remapRow(element, config, tableSchema, null);
    }

    static SeaTunnelRow remapRow(
            SeaTunnelRow element,
            JdbcSinkConfig config,
            TableSchema tableSchema,
            CodeConverter codeConverter) {
        if (!hasFieldMapping(config) && !Boolean.TRUE.equals(config.getRecordOperation())) {
            return element;
        }

        List<String> columns =
                tableSchema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        Object[] newFields = new Object[columns.size()];
        // Prefer configured value_mapper; do not re-derive against remapped sink schema.
        Map<String, String> valueMapper =
                config.getValueMapper() != null && !config.getValueMapper().isEmpty()
                        ? config.getValueMapper()
                        : effectiveValueMapper(config, tableSchema);
        Map<String, String> codeMapper = config.getCodeMapper();

        if (!valueMapper.isEmpty()) {
            for (int i = 0; i < columns.size(); i++) {
                String sourceIndex = getKeyByValue(valueMapper, columns.get(i));
                if (sourceIndex == null) {
                    continue;
                }
                Object sourceValue = element.getField(Integer.parseInt(sourceIndex));
                String code = codeMapper != null ? codeMapper.get(columns.get(i)) : null;
                if (code == null || codeConverter == null) {
                    newFields[i] = sourceValue;
                } else {
                    newFields[i] =
                            codeConverter.convert(
                                    code, sourceValue == null ? null : String.valueOf(sourceValue));
                }
            }
        } else {
            System.arraycopy(element.getFields(), 0, newFields, 0, element.getArity());
        }

        SeaTunnelRow newRow = new SeaTunnelRow(newFields);
        newRow.setRowKind(element.getRowKind());
        newRow.setTableId(element.getTableId());
        applyRecordOperation(newRow, element.getRowKind(), config);
        return newRow;
    }

    private static void applyRecordOperation(
            SeaTunnelRow row, RowKind rowKind, JdbcSinkConfig config) {
        if (!Boolean.TRUE.equals(config.getRecordOperation()) || row.getArity() < 2) {
            return;
        }
        int operationIndex = row.getArity() - 2;
        int timestampIndex = row.getArity() - 1;
        if (rowKind.equals(RowKind.DELETE)) {
            row.setField(operationIndex, "D");
        } else {
            row.setField(operationIndex, "U");
        }
        row.setField(timestampIndex, LocalDateTime.now());
    }

    private static void appendRecordOperationColumns(
            List<Column> sinkColumns, TableSchema databaseTableSchema) {
        if (databaseTableSchema != null && databaseTableSchema.getColumns().size() >= 2) {
            List<Column> dbColumns = databaseTableSchema.getColumns();
            Column opColumn = dbColumns.get(dbColumns.size() - 2);
            Column tsColumn = dbColumns.get(dbColumns.size() - 1);
            boolean hasOp =
                    sinkColumns.stream()
                            .anyMatch(c -> c.getName().equalsIgnoreCase(opColumn.getName()));
            boolean hasTs =
                    sinkColumns.stream()
                            .anyMatch(c -> c.getName().equalsIgnoreCase(tsColumn.getName()));
            if (!hasOp) {
                sinkColumns.add(opColumn.copy());
            }
            if (!hasTs) {
                sinkColumns.add(tsColumn.copy());
            }
            return;
        }
        // Fallback names when sink catalog is unavailable.
        sinkColumns.add(
                PhysicalColumn.of(
                        "OP", BasicType.STRING_TYPE, (Long) 1L, true, null, "operation type"));
        sinkColumns.add(
                PhysicalColumn.of(
                        "OP_TIME",
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        (Long) null,
                        true,
                        null,
                        "operation time"));
    }

    private static List<String> resolvePrimaryKeyNames(
            JdbcSinkConfig config, TableSchema tableSchema) {
        if (CollectionUtils.isNotEmpty(config.getPrimaryKeys())) {
            return new ArrayList<>(config.getPrimaryKeys());
        }
        if (tableSchema.getPrimaryKey() != null
                && CollectionUtils.isNotEmpty(tableSchema.getPrimaryKey().getColumnNames())) {
            return new ArrayList<>(tableSchema.getPrimaryKey().getColumnNames());
        }
        return Collections.emptyList();
    }

    private static Optional<String> resolvePrimaryKeyName(
            JdbcSinkConfig config, TableSchema tableSchema) {
        List<String> names = resolvePrimaryKeyNames(config, tableSchema);
        return names.isEmpty() ? Optional.empty() : Optional.of(names.get(0));
    }

    private static Integer resolveSourceFieldIndex(
            JdbcSinkConfig config, TableSchema tableSchema, String targetColumnName) {
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        int directIndex = rowType.indexOf(targetColumnName, false);
        if (directIndex >= 0) {
            return directIndex;
        }

        Map<String, String> valueMapper = effectiveValueMapper(config, tableSchema);
        String sourceIndex = getKeyByValue(valueMapper, targetColumnName);
        if (sourceIndex != null) {
            return Integer.parseInt(sourceIndex);
        }

        Map<String, String> fieldMapper = config.getFieldMapper();
        if (fieldMapper != null) {
            for (Map.Entry<String, String> entry : fieldMapper.entrySet()) {
                if (entry.getValue().equalsIgnoreCase(targetColumnName)) {
                    int index = rowType.indexOf(entry.getKey(), false);
                    if (index >= 0) {
                        return index;
                    }
                }
            }
        }
        return null;
    }

    private static Map<String, String> effectiveValueMapper(
            JdbcSinkConfig config, TableSchema tableSchema) {
        Map<String, String> valueMapper = config.getValueMapper();
        if (valueMapper != null && !valueMapper.isEmpty()) {
            return valueMapper;
        }
        Map<String, String> fieldMapper = config.getFieldMapper();
        if (fieldMapper == null || fieldMapper.isEmpty()) {
            return Collections.emptyMap();
        }
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        Map<String, String> derived = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : fieldMapper.entrySet()) {
            int idx = rowType.indexOf(entry.getKey(), false);
            if (idx >= 0) {
                derived.put(String.valueOf(idx), entry.getValue());
            }
        }
        return derived;
    }

    private static <K, V> K getKeyByValue(Map<K, V> map, V value) {
        if (map == null || value == null) {
            return null;
        }
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (value.equals(entry.getValue())
                    || (entry.getValue() instanceof String
                            && value instanceof String
                            && ((String) entry.getValue()).equalsIgnoreCase((String) value))) {
                return entry.getKey();
            }
        }
        return null;
    }
}
