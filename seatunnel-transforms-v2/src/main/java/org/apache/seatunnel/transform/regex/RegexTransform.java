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

package org.apache.seatunnel.transform.regex;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;

import com.google.auto.service.AutoService;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@AutoService(SeaTunnelTransform.class)
public class RegexTransform extends MultipleFieldOutputTransform {
    private RegexTransformConfig regexTransformConfig;
    private int regexFieldIndex;

    public RegexTransform(
            @NonNull RegexTransformConfig regexTransformConfig,
            @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.regexTransformConfig = regexTransformConfig;
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        regexFieldIndex = seaTunnelRowType.indexOf(regexTransformConfig.getRegexField());
        if (regexFieldIndex == -1) {
            throw new IllegalArgumentException(
                    "Cannot find ["
                            + regexTransformConfig.getRegexField()
                            + "] field in input row type");
        }
        this.outputCatalogTable = getProducedCatalogTable();
    }

    @Override
    public String getPluginName() {
        return "Regex";
    }

    //    @Override
    //    protected void setConfig(Config pluginConfig) {
    //        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig))
    //                .validate(new RegexTransformFactory().optionRule());
    //        this.regexTransformConfig =
    //                RegexTransformConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
    //    }

    //    @Override
    //    protected void setInputRowType(SeaTunnelRowType rowType) {
    //        String regexField = regexTransformConfig.getRegexField();
    //        if (regexField == null || regexField.trim().equals("")) {
    //            regexFieldIndex = 0;
    //        } else {
    //            regexFieldIndex = rowType.indexOf(regexField);
    //        }
    //        if (regexFieldIndex == -1) {
    //            throw new IllegalArgumentException(
    //                    "Cannot find ["
    //                            + regexField
    //                            + "] field in input row type");
    //        }
    //    }

    //    @Override
    //    protected String[] getOutputFieldNames() {
    //        return regexTransformConfig.getOutputFields();
    //    }
    //
    //    @Override
    //    protected SeaTunnelDataType[] getOutputFieldDataTypes() {
    //        return IntStream.range(0, regexTransformConfig.getOutputFields().length)
    //                .mapToObj((IntFunction<SeaTunnelDataType>) value -> BasicType.STRING_TYPE)
    //                .toArray(value -> new SeaTunnelDataType[value]);
    //    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object regexFieldValue = inputRow.getField(regexFieldIndex);
        if (regexFieldValue == null) {
            return regexTransformConfig.getEmptyRegexes();
        }

        Pattern pattern = Pattern.compile(regexTransformConfig.getRegex());
        Matcher matcher = pattern.matcher(regexFieldValue.toString());

        if (!matcher.find()) {
            return new Object[regexTransformConfig.getOutputFields().length];
        }
        String[] regexFieldValues = new String[regexTransformConfig.getOutputFields().length];
        for (int i = 0; i < regexTransformConfig.getOutputFields().length; i++) {
            regexFieldValues[i] = matcher.group(regexTransformConfig.getOutputFields()[i]);
        }
        return regexFieldValues;
    }

    @Override
    protected Column[] getOutputColumns() {
        List<PhysicalColumn> collect =
                Arrays.stream(regexTransformConfig.getOutputFields())
                        .map(
                                fieldName -> {
                                    return PhysicalColumn.of(
                                            fieldName, BasicType.STRING_TYPE, 200, true, "", "");
                                })
                        .collect(Collectors.toList());
        return collect.toArray(new Column[0]);
    }
}
