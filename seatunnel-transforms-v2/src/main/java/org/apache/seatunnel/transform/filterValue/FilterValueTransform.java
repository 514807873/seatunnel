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

package org.apache.seatunnel.transform.filterValue;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import org.apache.commons.collections4.CollectionUtils;

import cn.hutool.http.HttpUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.transform.filterValue.FilterValueRuleEnum.applyRules;

@Slf4j
public class FilterValueTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "FilterValue";
    private Map<String, Integer> columnIndex;
    private final RuleGroup ruleGroup;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;
    private final DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
    private DateTimeUtils.Formatter dateTimeFormatter =
            DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS;
    private final SeaTunnelRowType seaTunnelRowType;

    private final String subInsertCountUrl =
            System.getenv("ST_SERVICE_URL")
                    + "/SeaTunnelJob/subInsertCount/"
                    + System.getenv("seaTunnelJobId");
    private final String flinkDeleteDataUrl =
            System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/flinkDeleteData";

    public FilterValueTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        RuleGroup ruleGroup = config.get(FilterValueTransformConfig.FIELDS);

        // 收集所有规则中的字段名
        List<String> fieldNames = collectAllFieldNames(ruleGroup);

        // 检查字段是否存在
        List<String> canNotFoundFields =
                fieldNames.stream()
                        .filter(name -> seaTunnelRowType.indexOf(name, false) == -1)
                        .distinct()
                        .collect(Collectors.toList());

        if (!CollectionUtils.isEmpty(canNotFoundFields)) {
            throw TransformCommonError.cannotFindInputFieldsError(
                    getPluginName(), canNotFoundFields);
        }

        this.ruleGroup = ruleGroup;
    }

    // 递归收集所有规则中的字段名
    private List<String> collectAllFieldNames(RuleGroup ruleGroup) {
        List<String> fieldNames = new ArrayList<>();
        for (Object rule : ruleGroup.getRules()) {
            if (rule instanceof Rule) {
                fieldNames.add(((Rule) rule).getName());
            } else if (rule instanceof RuleGroup) {
                fieldNames.addAll(collectAllFieldNames((RuleGroup) rule));
            }
        }
        return fieldNames;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Boolean ruleResult = applyRules(ruleGroup, inputRow, columnIndex);
        // I类型数据 校验通不过 不往下游推
        if (inputRow.getRowKind().equals(RowKind.INSERT) && !ruleResult) {
            return null;
        }
        // I类型数据 校验通过 往下游推
        else if (inputRow.getRowKind().equals(RowKind.INSERT) && ruleResult) {
            return inputRow;
        }
        // U类型数据 校验通不过 往下游推删除操作
        // 由于转换不允许往下游推送RowKind.DELETE 类型的数据 需要将delete操作发送至中台执行
        else if (inputRow.getRowKind().equals(RowKind.UPDATE_AFTER) && !ruleResult) {
            deleteDataOnPanguSeaTunnel(inputRow);
            return null;
        }
        // U类型数据 校验通过 往下游推插入操作
        else if (inputRow.getRowKind().equals(RowKind.UPDATE_AFTER) && ruleResult) {
            inputRow.setRowKind(RowKind.INSERT);
            HttpUtil.get(subInsertCountUrl);
            return inputRow;
        } else if (inputRow.getRowKind().equals(RowKind.DELETE)) {
            deleteDataOnPanguSeaTunnel(inputRow);
            return null;
        } else {
            return inputRow;
        }
    }

    public String buildJsonString(String seaTunnelJobId, SeaTunnelRow row) throws IOException {
        Map<String, Object> rowMap = new HashMap<>(row.getFields().length);
        rowMap.put("xiJiaSeaTunnelJobId", seaTunnelJobId);
        for (int i = 0; i < row.getFields().length; i++) {
            Object value = convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            rowMap.put(seaTunnelRowType.getFieldName(i), value);
        }
        return objectMapper.writeValueAsString(rowMap);
    }

    private void deleteDataOnPanguSeaTunnel(SeaTunnelRow inputRow) {
        String seaTunnelJobId = System.getenv("seaTunnelJobId");
        String inputRowJson = null;
        try {
            inputRowJson = buildJsonString(seaTunnelJobId, inputRow);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            this.sendPostRequest(flinkDeleteDataUrl, inputRowJson);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sendPostRequest(String url, String data) throws Exception {
        URL apiUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setDoOutput(true);
        OutputStream outputStream = connection.getOutputStream();
        outputStream.write(data.getBytes());
        outputStream.flush();
        outputStream.close();
        // 处理响应
        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            // 成功处理响应
        } else {
            // 处理失败响应
        }
        connection.disconnect();
    }

    protected Object convert(SeaTunnelDataType dataType, Object val) {
        if (val == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BOOLEAN:
            case STRING:
                return val;
            case DATE:
                return DateUtils.toString((LocalDate) val, dateFormatter);
            case TIME:
                return TimeUtils.toString((LocalTime) val, timeFormatter);
            case TIMESTAMP:
                return DateTimeUtils.toString((LocalDateTime) val, dateTimeFormatter);
            case ARRAY:
            case MAP:
                return JsonUtils.toJsonString(val);
            case BYTES:
                return new String((byte[]) val);
            default:
                throw new RuntimeException("不支持的类型:" + dataType.getSqlType());
        }
    }

    @Override
    protected TableSchema transformTableSchema() {
        columnIndex = new HashMap<>();
        SeaTunnelRowType seaTunnelRowType =
                inputCatalogTable.getTableSchema().toPhysicalRowDataType();

        // 递归处理规则组中的所有规则
        processRules(ruleGroup, seaTunnelRowType);

        return inputCatalogTable.getTableSchema();
    }

    // 递归处理方法
    private void processRules(RuleGroup ruleGroup, SeaTunnelRowType seaTunnelRowType) {
        for (Object ruleObj : ruleGroup.getRules()) {
            ObjectMapper objectMapper = new ObjectMapper();
            if (ruleObj instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) ruleObj;
                if (map.containsKey("logicalOperator") && map.containsKey("rules")) {
                    // 是个条件组  是个RuleGroup
                    RuleGroup subGroup = objectMapper.convertValue(map, RuleGroup.class);
                    // 递归处理子规则组
                    processRules(subGroup, seaTunnelRowType);
                } else if (map.containsKey("name")
                        && map.containsKey("operator")
                        && map.containsKey("value")) {
                    // 是条件 Rule
                    Rule rule = objectMapper.convertValue(map, Rule.class);
                    int inputFieldIndex = seaTunnelRowType.indexOf(rule.getName());
                    columnIndex.put(rule.getName(), inputFieldIndex);
                } else {
                    throw new IllegalArgumentException("Unknown rule structure: " + map);
                }
            } else {
                throw new IllegalArgumentException("Unknown rule type: " + ruleObj.getClass());
            }
        }
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
