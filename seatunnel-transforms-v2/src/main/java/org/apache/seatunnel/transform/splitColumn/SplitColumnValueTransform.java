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

package org.apache.seatunnel.transform.splitColumn;

import cn.hutool.json.JSONObject;
import lombok.NonNull;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SplitColumnValueTransform extends AbstractCatalogSupportTransform {
    private final SplitColumnValueTransformConfig splitTransformConfig;
    private List<String> fieldNames;
    private List<SeaTunnelDataType<?>> fieldTypes;
    private List<SplitConfig> splitConfigs;
    private List<Column> outputColumns = new ArrayList<>();

    public SplitColumnValueTransform(
            @NonNull SplitColumnValueTransformConfig splitTransformConfig,
            @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        initOutputFields(seaTunnelRowType);
        this.splitTransformConfig = splitTransformConfig;
        this.splitConfigs = parseSplitConfigs();
    }

    private void initOutputFields(SeaTunnelRowType inputRowType) {
        this.fieldNames = Arrays.stream(inputRowType.getFieldNames()).collect(Collectors.toList());
        this.fieldTypes = Arrays.stream(inputRowType.getFieldTypes()).collect(Collectors.toList());
    }

    private List<SplitConfig> parseSplitConfigs() {
        List<SplitConfig> configs = new ArrayList<>();
        List<JSONObject> separator = splitTransformConfig.getSeparator();
        for (JSONObject config : separator) {
            SplitConfig splitConfig = new SplitConfig();
            splitConfig.oldColumn = config.getStr("oldColumn");
            splitConfig.newColumn = config.getStr("newColumn");
            splitConfig.split = config.getStr("split");
            splitConfig.comment = config.getStr("comment");
            splitConfig.example = config.getStr("example");
            configs.add(splitConfig);
        }
        return configs;
    }

    @Override
    public String getPluginName() {
        return "SplitColumnValue";
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return null;
    }

    @Override
    public List<SeaTunnelRow> mapList(SeaTunnelRow inputRow) {
        List<SeaTunnelRow> rows = new ArrayList<>();

        if (splitConfigs.isEmpty()) {
            // 没有拆分配置，直接返回原始行
            Object[] fields = new Object[outputColumns.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                fields[i] = inputRow.getField(i);
            }
            // 新列设为空值
            for (int i = fieldNames.size(); i < outputColumns.size(); i++) {
                fields[i] = null;
            }
            SeaTunnelRow newRow = new SeaTunnelRow(fields);
            newRow.setTableId(inputRow.getTableId());
            newRow.setRowKind(inputRow.getRowKind());
            rows.add(newRow);
            return rows;
        }

        // 动态处理所有拆分配置，生成笛卡尔积
        List<List<SplitValue>> allSplitValues = new ArrayList<>();

        for (SplitConfig config : splitConfigs) {
            String fieldValue = getFieldValueAsString(inputRow, config.oldColumn);
            String[] splitValues = fieldValue.split(config.split, -1);
            List<SplitValue> valuesList = new ArrayList<>();
            for (int i = 0; i < splitValues.length; i++) {
                valuesList.add(new SplitValue(config.newColumn, splitValues[i].trim(), i));
            }
            allSplitValues.add(valuesList);
        }

        // 生成笛卡尔积
        generateCartesianProduct(inputRow, allSplitValues, 0, new HashMap<>(), rows);

        return rows;
    }

    private void generateCartesianProduct(SeaTunnelRow inputRow,
                                          List<List<SplitValue>> allSplitValues,
                                          int configIndex,
                                          Map<String, String> currentCombination,
                                          List<SeaTunnelRow> rows) {
        if (configIndex == allSplitValues.size()) {
            // 生成一行数据
            Object[] fields = new Object[outputColumns.size()];

            // 复制原始字段
            for (int i = 0; i < fieldNames.size(); i++) {
                fields[i] = inputRow.getField(i);
            }

            // 设置新字段的值
            for (int i = 0; i < splitConfigs.size(); i++) {
                String columnName = splitConfigs.get(i).newColumn;
                String value = currentCombination.getOrDefault(columnName, "");
                fields[fieldNames.size() + i] = value;
            }

            SeaTunnelRow newRow = new SeaTunnelRow(fields);
            newRow.setTableId(inputRow.getTableId());
            newRow.setRowKind(inputRow.getRowKind());
            rows.add(newRow);
            return;
        }

        // 递归生成笛卡尔积
        List<SplitValue> currentValues = allSplitValues.get(configIndex);
        for (SplitValue splitValue : currentValues) {
            currentCombination.put(splitValue.columnName, splitValue.value);
            generateCartesianProduct(inputRow, allSplitValues, configIndex + 1, currentCombination, rows);
            currentCombination.remove(splitValue.columnName);
        }

        // 如果当前配置没有值，也要继续处理
        if (currentValues.isEmpty()) {
            generateCartesianProduct(inputRow, allSplitValues, configIndex + 1, currentCombination, rows);
        }
    }

    private static class SplitValue {
        String columnName;
        String value;
        int index;

        SplitValue(String columnName, String value, int index) {
            this.columnName = columnName;
            this.value = value;
            this.index = index;
        }
    }

    private String getFieldValueAsString(SeaTunnelRow row, String fieldName) {
        int index = getFieldIndex(fieldName);
        Object value = row.getField(index);
        return value == null ? "" : value.toString();
    }

    private int getFieldIndex(String fieldName) {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldNames.get(i).equals(fieldName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not found: " + fieldName);
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<Column> outputColumns = new ArrayList<>();

        // 添加所有原始列
        for (int i = 0; i < fieldNames.size(); i++) {
            outputColumns.add(PhysicalColumn.of(fieldNames.get(i), fieldTypes.get(i), 200, true, "", ""));
        }

        // 添加新列
        for (SplitConfig config : splitConfigs) {
            outputColumns.add(PhysicalColumn.of(config.newColumn, BasicType.STRING_TYPE, 200, true, config.comment, ""));
        }

        this.outputColumns = outputColumns;
        return TableSchema.builder()
                .columns(outputColumns)
                .build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    private static class SplitConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        String oldColumn;
        String newColumn;
        String split;
        String comment;
        String example;
    }
}