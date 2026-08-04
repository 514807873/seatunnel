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

package com.qh.sqlcdc.compare;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Locale;

/**
 * 判定主键是否为跨库一致可排序的数字类型（含整数与小数），用于选择双游标或 Map 对账。
 */
public final class NumericTypeDetector {

    private NumericTypeDetector() {}

    public static boolean allSourcePkNumeric(SeaTunnelRowType rowType, List<String> sourcePks) {
        if (sourcePks == null || sourcePks.isEmpty()) {
            return false;
        }
        for (String pk : sourcePks) {
            int idx = indexOfIgnoreCase(rowType, pk);
            if (idx < 0) {
                return false;
            }
            if (!isNumeric(rowType.getFieldType(idx))) {
                return false;
            }
        }
        return true;
    }

    public static boolean allColumnsNumeric(ResultSetMetaData meta, List<String> columnLabels)
            throws SQLException {
        if (columnLabels == null || columnLabels.isEmpty()) {
            return false;
        }
        for (String label : columnLabels) {
            int idx = findColumnIndex(meta, label);
            if (idx < 0 || !isNumeric(meta, idx)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isNumeric(SeaTunnelDataType<?> dataType) {
        if (dataType == null) {
            return false;
        }
        SqlType sqlType = dataType.getSqlType();
        return sqlType == SqlType.TINYINT
                || sqlType == SqlType.SMALLINT
                || sqlType == SqlType.INT
                || sqlType == SqlType.BIGINT
                || sqlType == SqlType.FLOAT
                || sqlType == SqlType.DOUBLE
                || sqlType == SqlType.DECIMAL;
    }

    public static boolean isNumeric(ResultSetMetaData meta, int colIndex) throws SQLException {
        int jdbcType = meta.getColumnType(colIndex);
        if (isJdbcNumericType(jdbcType)) {
            return true;
        }
        return isNumericTypeName(meta.getColumnTypeName(colIndex));
    }

    public static boolean isJdbcNumericType(int jdbcType) {
        switch (jdbcType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return true;
            default:
                return false;
        }
    }

    /**
     * 覆盖 MySQL / PG / Oracle / SQLServer / ClickHouse 等常见数字类型名。
     */
    public static boolean isNumericTypeName(String typeName) {
        if (typeName == null || typeName.isEmpty()) {
            return false;
        }
        String t = typeName.trim().toLowerCase(Locale.ROOT);
        // 去掉 Nullable(xxx) / LowCardinality(xxx) 等包装
        t = unwrapClickHouseType(t);
        if (t.startsWith("decimal")
                || t.startsWith("numeric")
                || t.startsWith("number")
                || t.startsWith("money")
                || t.startsWith("smallmoney")) {
            return true;
        }
        switch (t) {
            case "tinyint":
            case "tinyint unsigned":
            case "smallint":
            case "smallint unsigned":
            case "mediumint":
            case "mediumint unsigned":
            case "int":
            case "integer":
            case "int unsigned":
            case "integer unsigned":
            case "bigint":
            case "bigint unsigned":
            case "int2":
            case "int4":
            case "int8":
            case "serial":
            case "bigserial":
            case "smallserial":
            case "float":
            case "float4":
            case "float8":
            case "real":
            case "double":
            case "double precision":
            case "binary_float":
            case "binary_double":
            case "uint8":
            case "int16":
            case "uint16":
            case "int32":
            case "uint32":
            case "uint64":
            case "int128":
            case "uint128":
            case "int256":
            case "uint256":
            case "float32":
            case "float64":
            case "int64":
                return true;
            default:
                // ClickHouse Int128 / UInt64 / Float32 等；避免 interval 等误匹配
                return t.matches("u?int\\d+") || t.matches("float\\d+");
        }
    }

    private static String unwrapClickHouseType(String t) {
        String current = t;
        while (true) {
            int open = current.indexOf('(');
            if (open <= 0 || !current.endsWith(")")) {
                break;
            }
            String wrapper = current.substring(0, open);
            if ("nullable".equals(wrapper)
                    || "lowcardinality".equals(wrapper)
                    || "simpleaggregatefunction".equals(wrapper)) {
                current = current.substring(open + 1, current.length() - 1).trim();
                continue;
            }
            // Decimal(18, 2) → decimal
            if ("decimal".equals(wrapper) || "decimal32".equals(wrapper)
                    || "decimal64".equals(wrapper)
                    || "decimal128".equals(wrapper)
                    || "decimal256".equals(wrapper)) {
                return wrapper;
            }
            break;
        }
        return current;
    }

    private static int indexOfIgnoreCase(SeaTunnelRowType rowType, String fieldName) {
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (rowType.getFieldName(i).equalsIgnoreCase(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    private static int findColumnIndex(ResultSetMetaData meta, String label) throws SQLException {
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (meta.getColumnLabel(i).equalsIgnoreCase(label)
                    || meta.getColumnName(i).equalsIgnoreCase(label)) {
                return i;
            }
        }
        return -1;
    }
}
