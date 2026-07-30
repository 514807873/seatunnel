package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import org.apache.commons.collections4.CollectionUtils;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@AutoService(SeaTunnelTransform.class)
public class EncryptTransform extends AbstractCatalogSupportMapTransform {
    public static String PLUGIN_NAME = "Encrypt";
    private EncryptTransformConfig config;

    private List<Integer> needEncryptColIndex;
    private static final String KEY = "86C63180C2806ED1F47B859DE501215B";

    public EncryptTransform(
            @NonNull EncryptTransformConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
        List<String> encryptColumns = config.getEncryptColumns();
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        List<String> notFoundField =
                encryptColumns.stream()
                        .filter(field -> seaTunnelRowType.indexOf(field) == -1)
                        .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(notFoundField)) {
            throw TransformCommonError.cannotFindInputFieldError(
                    getPluginName(), notFoundField.toString());
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        List<String> encryptColumns = config.getEncryptColumns();
        needEncryptColIndex = new ArrayList<>(encryptColumns.size());
        ArrayList<String> inputFieldNames = Lists.newArrayList(inputRowType.getFieldNames());
        encryptColumns.forEach(
                x -> {
                    int i = inputFieldNames.indexOf(x);
                    needEncryptColIndex.add(i);
                });
        return inputRowType;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] fields = inputRow.getFields();
        Object[] outputDataArray = new Object[fields.length];
        for (int i = 0; i < outputDataArray.length; i++) {
            if (needEncryptColIndex.contains(i)) {
                String encryptHex = null;
                try {
                    if (null != inputRow.getField(i)) {
                        encryptHex = SM4Util.encryptEcb(KEY, (String) inputRow.getField(i));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                outputDataArray[i] = encryptHex;
            } else {
                outputDataArray[i] = inputRow.getField(i);
            }
        }
        SeaTunnelRow outputRow = new SeaTunnelRow(outputDataArray);
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setTableId(inputRow.getTableId());
        return outputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<String> encryptColumns = config.getEncryptColumns();
        needEncryptColIndex = new ArrayList<>(encryptColumns.size());
        List<Column> inputColumns = inputCatalogTable.getTableSchema().getColumns();
        for (int i = 0; i < inputColumns.size(); i++) {
            Column column = inputColumns.get(i);
            if (encryptColumns.contains(column.getName())) {
                needEncryptColIndex.add(i);
            }
        }
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
