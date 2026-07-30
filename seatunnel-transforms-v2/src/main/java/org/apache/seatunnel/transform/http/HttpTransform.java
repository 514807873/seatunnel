package org.apache.seatunnel.transform.http;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.auto.service.AutoService;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@AutoService(SeaTunnelTransform.class)
public class HttpTransform extends AbstractCatalogSupportTransform {
    public static String PLUGIN_NAME = "http_transform";
    private HttpTransformConfig config;
    private List<Column> outputColumns = new ArrayList<>();
    private int subtaskIndex = -1;
    private static final Configuration jsonConfiguration =
            Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL);

    public HttpTransform(
            @NonNull HttpTransformConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void setSubtaskIndex(int subtaskIndex) {
        this.subtaskIndex = subtaskIndex;
    }


    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return null;
    }

    @Override
    public List<SeaTunnelRow> mapList(SeaTunnelRow inputRow) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        List<String> columns =
                Arrays.stream(this.inputCatalogTable.getTableSchema().toPhysicalRowDataType().getFieldNames()).collect(Collectors.toList());
        JSONObject obj = JSONUtil.createObj();
        boolean needPage = false;
        if (config.getPagesParams() != null) {
            obj.putAll(config.getPagesParams());
            needPage = true;
        }
        if (config.getParamsField() != null) {
            config.getParamsField().forEach(param -> {
                if (param.getType().equals("fixed")) {
                    obj.putOpt(param.getName(), param.getValue());
                }
                else if (param.getType().equals("dynamic")) {
                    int valueIndex = columns.indexOf(param.getValue());
                    if (valueIndex < 0) throw new RuntimeException(String.format("输入字段%s未找到", param.getValue()));
                    Object field = inputRow.getField(valueIndex);
                    obj.putOpt(param.getName(), field);
                }
            });
        }
        if (needPage) {
            Map<String, String> pagesParams = config.getPagesParams();
            obj.putOpt("size", pagesParams.get("size"));
            //分页查询
            if (pagesParams.containsKey("current")) {
                int loop_count = 1;
                while (loop_count > 0) {
                    try (HttpResponse response = HttpRequest.post(config.getUrl())
                            .header("Content-Type", "application/json")
                            .body(obj.toString())
                            .execute()) {
                        if (response.isOk()) {
                            Map<String, List<String>> dataSet = new HashMap<>();
                            ReadContext ctx = JsonPath.using(jsonConfiguration).parse(response.body());
                            Map<String, String> jsonField = config.getJsonField();
                            if (jsonField != null) {
                                for (Map.Entry<String, String> entry : jsonField.entrySet()) {
                                    String key = entry.getKey();
                                    String value = entry.getValue();
                                    List<?> valuesList = ctx.read(value);
                                    if (key.startsWith("cspz_") && areAllNull(valuesList)) {
                                        List<String> newValuesList = new ArrayList<>();
                                        valuesList.forEach(item -> {
                                            newValuesList.add(obj.getStr(key.replace("cspz_", "")));
                                        });
                                        dataSet.put(key, newValuesList);
                                    }
                                    else {
                                        dataSet.put(key, toStringList(valuesList));
                                    }
                                }
                            }
                            for (int i = 0; i < new ArrayList<>(dataSet.values()).get(0).size(); i++) {
                                Object[] fields = new Object[outputColumns.size()];
                                for (int j = 0; j < outputColumns.size(); j++) {
                                    String columName = outputColumns.get(j).getName();
                                    fields[j] = StrUtil.toString(dataSet.get(columName).get(i));
                                }
                                rows.add(new SeaTunnelRow(fields));
                            }
                            log.info("subtask={}, 请求参数{}", subtaskIndex, obj);
                            loop_count++;
                            if (new ArrayList<>(dataSet.values()).get(0).size() < Long.parseLong(pagesParams.get("size"))) {
                                loop_count = 0;
                                log.info("没有更多数据,总共" + rows.size() + "条数据");
                            }
                            obj.putOpt("current", loop_count + "");
                        }
                        else {
                            log.error("Error: " + response.getStatus() + ", " + response.body());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            //偏移量查询
            if (pagesParams.containsKey("offset")) {
                obj.putOpt("offset", pagesParams.get("offset"));
            }
        }
        else {
            try (HttpResponse response = HttpRequest.post(config.getUrl())
                    .header("Content-Type", "application/json")
                    .body(obj.toString())
                    .execute()) {
                if (response.isOk()) {
                    Map<String, List<String>> dataSet = new HashMap<>();
                    ReadContext ctx = JsonPath.using(jsonConfiguration).parse(response.body());
                    Map<String, String> jsonField = config.getJsonField();
                    if (jsonField != null) {
                        for (Map.Entry<String, String> entry : jsonField.entrySet()) {
                            String key = entry.getKey();
                            String value = entry.getValue();
                            List<?> valuesList = ctx.read(value);
                            if (key.startsWith("cspz_") && areAllNull(valuesList)) {
                                List<String> newValuesList = new ArrayList<>();
                                valuesList.forEach(item -> {
                                    newValuesList.add(obj.getStr(key.replace("cspz_", "")));
                                });
                                dataSet.put(key, newValuesList);
                            }
                            else {
                                dataSet.put(key, toStringList(valuesList));
                            }
                        }
                    }
                    for (int i = 0; i < new ArrayList<>(dataSet.values()).get(0).size(); i++) {
                        Object[] fields = new Object[outputColumns.size()];
                        for (int j = 0; j < outputColumns.size(); j++) {
                            String columName = outputColumns.get(j).getName();
                            fields[j] = StrUtil.toString(dataSet.get(columName).get(i));
                        }
                        rows.add(new SeaTunnelRow(fields));
                    }
                    log.info("subtask={}, 请求参数{}", subtaskIndex, obj);
                }
                else {
                    log.error("Error: " + response.getStatus() + ", " + response.body());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return rows;
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<Column> outputColumns = new ArrayList<>();
        Map<String, String> jsonField = config.getJsonField();
        if (jsonField != null) {
            for (Map.Entry<String, String> entry : jsonField.entrySet()) {
                String key = entry.getKey();
                outputColumns.add(PhysicalColumn.of(
                        key, BasicType.STRING_TYPE, 1000, true, "", ""));
            }
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

    private boolean areAllNull(List<?> list) {
        if (list == null || list.isEmpty()) {
            return true;
        }
        for (Object element : list) {
            if (element != null) {
                return false;
            }
        }
        return true;
    }

    private List<String> toStringList(List<?> list) {
        List<String> result = new ArrayList<>();
        if (list == null) {
            return result;
        }
        for (Object element : list) {
            result.add(element == null ? null : StrUtil.toString(element));
        }
        return result;
    }
}
