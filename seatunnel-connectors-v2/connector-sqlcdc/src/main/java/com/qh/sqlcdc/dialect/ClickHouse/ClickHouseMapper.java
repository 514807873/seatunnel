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

package com.qh.sqlcdc.dialect.ClickHouse;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ClickHouseMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // ============================data types=====================

    private static final String CLICKHOUSE_UNKNOWN = "UNKNOWN";
    private static final String CLICKHOUSE_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String CLICKHOUSE_TINYINT = "TINYINT";
    private static final String CLICKHOUSE_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String CLICKHOUSE_SMALLINT = "SMALLINT";
    private static final String CLICKHOUSE_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String CLICKHOUSE_MEDIUMINT = "MEDIUMINT";
    private static final String CLICKHOUSE_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String CLICKHOUSE_INT = "INT";
    private static final String CLICKHOUSE_INT_UNSIGNED = "INT UNSIGNED";
    private static final String CLICKHOUSE_INTEGER = "INTEGER";
    private static final String CLICKHOUSE_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String CLICKHOUSE_BIGINT = "BIGINT";
    private static final String CLICKHOUSE_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String CLICKHOUSE_DECIMAL = "DECIMAL";
    private static final String CLICKHOUSE_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String CLICKHOUSE_FLOAT = "FLOAT";
    private static final String CLICKHOUSE_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String CLICKHOUSE_DOUBLE = "DOUBLE";
    private static final String CLICKHOUSE_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String CLICKHOUSE_UINT_NULLABLE = "NULLABLE(UINT)";
    private static final String CLICKHOUSE_UINT8_NULLABLE = "NULLABLE(UINT8)";
    private static final String CLICKHOUSE_UINT16_NULLABLE = "NULLABLE(UINT16)";
    private static final String CLICKHOUSE_UINT32_NULLABLE = "NULLABLE(UINT32)";
    private static final String CLICKHOUSE_UINT64_NULLABLE = "NULLABLE(UINT64)";
    private static final String CLICKHOUSE_UINT128_NULLABLE = "NULLABLE(UINT128)";

    // -------------------------string----------------------------
    private static final String CLICKHOUSE_STRING = "STRING";
    private static final String CLICKHOUSE_NULLABLE_STRING = "NULLABLE(STRING)";
    private static final String CLICKHOUSE_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String CLICKHOUSE_DATE = "DATE";
    private static final String CLICKHOUSE_DATE32 = "DATE32";
    private static final String CLICKHOUSE_DATETIME = "DATETIME";
    private static final String CLICKHOUSE_TIME = "TIME";
    private static final String CLICKHOUSE_TIMESTAMP = "TIMESTAMP";
    private static final String CLICKHOUSE_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String CLICKHOUSE_TINYBLOB = "TINYBLOB";
    private static final String CLICKHOUSE_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String CLICKHOUSE_BLOB = "BLOB";
    private static final String CLICKHOUSE_LONGBLOB = "LONGBLOB";
    private static final String CLICKHOUSE_BINARY = "BINARY";
    private static final String CLICKHOUSE_VARBINARY = "VARBINARY";
    private static final String CLICKHOUSE_GEOMETRY = "GEOMETRY";

    private static final String CLICKHOUSE_UINT64 = "UINT64";
    private static final String CLICKHOUSE_UUID = "UUID";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (mysqlType) {
            case CLICKHOUSE_BIT:
                if (precision == 1) {
                    return BasicType.BOOLEAN_TYPE;
                } else {
                    return PrimitiveByteArrayType.INSTANCE;
                }
            case CLICKHOUSE_UINT_NULLABLE:
            case CLICKHOUSE_UINT8_NULLABLE:
            case CLICKHOUSE_UINT16_NULLABLE:
            case CLICKHOUSE_UINT32_NULLABLE:
            case CLICKHOUSE_UINT64_NULLABLE:
            case CLICKHOUSE_UINT128_NULLABLE:
            case CLICKHOUSE_TINYINT:
            case CLICKHOUSE_TINYINT_UNSIGNED:
            case CLICKHOUSE_SMALLINT:
            case CLICKHOUSE_SMALLINT_UNSIGNED:
            case CLICKHOUSE_MEDIUMINT:
            case CLICKHOUSE_MEDIUMINT_UNSIGNED:
            case CLICKHOUSE_INT:
            case CLICKHOUSE_INTEGER:
            case CLICKHOUSE_YEAR:
                return BasicType.INT_TYPE;
            case CLICKHOUSE_INT_UNSIGNED:
            case CLICKHOUSE_INTEGER_UNSIGNED:
            case CLICKHOUSE_BIGINT:
            case CLICKHOUSE_UINT64:
                return BasicType.LONG_TYPE;
            case CLICKHOUSE_BIGINT_UNSIGNED:
                return new DecimalType(20, 0);
            case CLICKHOUSE_DECIMAL:
                if (precision > 38) {
                    LOG.warn("{} will probably cause value overflow.", CLICKHOUSE_DECIMAL);
                    return new DecimalType(38, 18);
                }
                return new DecimalType(precision, scale);
            case CLICKHOUSE_DECIMAL_UNSIGNED:
                return new DecimalType(precision + 1, scale);
            case CLICKHOUSE_FLOAT:
                return BasicType.FLOAT_TYPE;
            case CLICKHOUSE_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", CLICKHOUSE_FLOAT_UNSIGNED);
                return BasicType.FLOAT_TYPE;
            case CLICKHOUSE_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case CLICKHOUSE_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", CLICKHOUSE_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE_TYPE;
            case CLICKHOUSE_STRING:
            case CLICKHOUSE_NULLABLE_STRING:
            case CLICKHOUSE_UUID:
            case CLICKHOUSE_JSON:
                return BasicType.STRING_TYPE;
            case CLICKHOUSE_DATE:
            case CLICKHOUSE_DATE32:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case CLICKHOUSE_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case CLICKHOUSE_DATETIME:
            case CLICKHOUSE_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case CLICKHOUSE_TINYBLOB:
            case CLICKHOUSE_MEDIUMBLOB:
            case CLICKHOUSE_BLOB:
            case CLICKHOUSE_LONGBLOB:
            case CLICKHOUSE_VARBINARY:
            case CLICKHOUSE_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

                // Doesn't support yet
            case CLICKHOUSE_GEOMETRY:
            case CLICKHOUSE_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new RuntimeException(
                        String.format(
                                "Doesn't support CLickHouse type '%s' on column '%s'  yet.",
                                mysqlType, jdbcColumnName));
        }
    }
}
