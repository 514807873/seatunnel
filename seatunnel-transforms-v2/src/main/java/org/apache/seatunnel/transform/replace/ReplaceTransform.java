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

package org.apache.seatunnel.transform.replace;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import com.google.auto.service.AutoService;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@AutoService(SeaTunnelTransform.class)
public class ReplaceTransform extends AbstractCatalogSupportMapTransform {
    private ReplaceTransformConfig config;
    private List<XjReplaceConfig> replaceFields;

    private Map<String, List<XjReplaceConfig>> fieldsMap;

    private final Map<String, Integer> filedsValueIndexMap = new HashMap<>();

    public ReplaceTransform(
            @NonNull ReplaceTransformConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
        this.replaceFields = config.getReplaceFields();
        SeaTunnelRowType inputRowType =
                this.inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        for (int i = 0; i < inputRowType.getFieldNames().length; i++) {
            String fieldName = inputRowType.getFieldName(i);
            int finalI = i;
            this.replaceFields.stream()
                    .filter(x -> x.getColumnName().equalsIgnoreCase(fieldName))
                    .forEach(x -> x.setValueIndex(finalI));
        }
        this.fieldsMap =
                this.replaceFields.stream()
                        .collect(Collectors.groupingBy(XjReplaceConfig::getColumnName));
        this.fieldsMap.forEach(
                (k, v) -> {
                    filedsValueIndexMap.put(k, v.get(0).getValueIndex());
                });
    }

    @Override
    public String getPluginName() {
        return "Replace";
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    //    @Override
    //    protected void setConfig(Config pluginConfig) {
    //        this.config = ReplaceTransformConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
    //        this.replaceFields = this.config.getReplaceFields();
    //        this.fieldsMap =
    // this.replaceFields.stream().collect(Collectors.groupingBy(XjReplaceConfig::getColumnName));
    //    }

    //    @Override
    //    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
    //        for (int i = 0; i < inputRowType.getFieldNames().length; i++) {
    //            String fieldName = inputRowType.getFieldName(i);
    //            int finalI = i;
    //            this.replaceFields.stream().filter(x ->
    // x.getColumnName().equalsIgnoreCase(fieldName)).forEach(x -> x.setValueIndex(finalI));
    //        }
    //        this.fieldsMap.forEach((k, v) -> {
    //            filedsValueIndexMap.put(k, v.get(0).getValueIndex());
    //        });
    //        return inputRowType;
    //    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] fields = inputRow.getFields();
        Object[] outputDataArray = new Object[fields.length];
        for (int i = 0; i < outputDataArray.length; i++) {
            int finalI = i;
            if (filedsValueIndexMap.containsValue(i)) {
                Object fieldValue = inputRow.getField(i);
                if (fieldValue != null) {
                    String value = inputRow.getField(i).toString().replace("\0", "");
                    String columnName = null;
                    Optional<String> first =
                            filedsValueIndexMap.entrySet().stream()
                                    .filter(x -> x.getValue().equals(finalI))
                                    .map(Map.Entry::getKey)
                                    .findFirst();
                    if (first.isPresent()) {
                        columnName = first.get();
                    }
                    List<XjReplaceConfig> list = fieldsMap.get(columnName);
                    for (XjReplaceConfig xjReplaceConfig : list) {
                        value =
                                value.replace(
                                        xjReplaceConfig.getOldString(),
                                        xjReplaceConfig.getNewString());
                    }
                    outputDataArray[finalI] = value;
                } else {
                    outputDataArray[finalI] = null;
                }
            } else {
                outputDataArray[finalI] = inputRow.getField(i);
            }
        }
        SeaTunnelRow outputRow = new SeaTunnelRow(outputDataArray);
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setTableId(inputRow.getTableId());
        return outputRow;
    }
}
