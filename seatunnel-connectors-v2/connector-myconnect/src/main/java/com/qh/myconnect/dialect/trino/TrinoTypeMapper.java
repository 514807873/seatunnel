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

package com.qh.myconnect.dialect.trino;

import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TrinoTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // ============================data types=====================

    private static final String TRINO_UNKNOWN = "UNKNOWN";
    private static final String TRINO_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String TRINO_TINYINT = "TINYINT";
    private static final String TRINO_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String TRINO_SMALLINT = "SMALLINT";
    private static final String TRINO_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String TRINO_MEDIUMINT = "MEDIUMINT";
    private static final String TRINO_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String TRINO_INT = "INT";
    private static final String TRINO_INT_UNSIGNED = "INT UNSIGNED";
    private static final String TRINO_INTEGER = "INTEGER";
    private static final String TRINO_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String TRINO_BIGINT = "BIGINT";
    private static final String TRINO_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String TRINO_DECIMAL = "DECIMAL";
    private static final String TRINO_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String TRINO_FLOAT = "FLOAT";
    private static final String TRINO_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String TRINO_DOUBLE = "DOUBLE";
    private static final String TRINO_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String TRINO_CHAR = "CHAR";
    private static final String TRINO_VARCHAR = "VARCHAR";
    private static final String TRINO_TINYTEXT = "TINYTEXT";
    private static final String TRINO_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TRINO_TEXT = "TEXT";
    private static final String TRINO_LONGTEXT = "LONGTEXT";
    private static final String TRINO_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String TRINO_DATE = "DATE";
    private static final String TRINO_DATETIME = "DATETIME";
    private static final String TRINO_TIME = "TIME";
    private static final String TRINO_TIMESTAMP = "TIMESTAMP";
    private static final String TRINO_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String TRINO_TINYBLOB = "TINYBLOB";
    private static final String TRINO_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String TRINO_BLOB = "BLOB";
    private static final String TRINO_LONGBLOB = "LONGBLOB";
    private static final String TRINO_BINARY = "BINARY";
    private static final String TRINO_VARBINARY = "VARBINARY";
    private static final String TRINO_GEOMETRY = "GEOMETRY";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String TRINOType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (TRINOType) {
            case TRINO_BIT:
                if (precision == 1) {
                    return BasicType.BOOLEAN_TYPE;
                } else {
                    return PrimitiveByteArrayType.INSTANCE;
                }
            case TRINO_TINYINT:
            case TRINO_TINYINT_UNSIGNED:
            case TRINO_SMALLINT:
            case TRINO_SMALLINT_UNSIGNED:
            case TRINO_MEDIUMINT:
            case TRINO_MEDIUMINT_UNSIGNED:
            case TRINO_INT:
            case TRINO_INTEGER:
            case TRINO_YEAR:
                return BasicType.INT_TYPE;
            case TRINO_INT_UNSIGNED:
            case TRINO_INTEGER_UNSIGNED:
            case TRINO_BIGINT:
                return BasicType.LONG_TYPE;
            case TRINO_BIGINT_UNSIGNED:
                return new DecimalType(20, 0);
            case TRINO_DECIMAL:
                if (precision > 38) {
                    LOG.warn("{} will probably cause value overflow.", TRINO_DECIMAL);
                    return new DecimalType(38, 18);
                }
                return new DecimalType(precision, scale);
            case TRINO_DECIMAL_UNSIGNED:
                return new DecimalType(precision + 1, scale);
            case TRINO_FLOAT:
                return BasicType.FLOAT_TYPE;
            case TRINO_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", TRINO_FLOAT_UNSIGNED);
                return BasicType.FLOAT_TYPE;
            case TRINO_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case TRINO_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", TRINO_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE_TYPE;
            case TRINO_CHAR:
            case TRINO_TINYTEXT:
            case TRINO_MEDIUMTEXT:
            case TRINO_TEXT:
            case TRINO_VARCHAR:
            case TRINO_JSON:
                return BasicType.STRING_TYPE;
            case TRINO_LONGTEXT:
                LOG.warn(
                        "Type '{}' has a maximum precision of 536870911 in TRINO. "
                                + "Due to limitations in the seatunnel type system, "
                                + "the precision will be set to 2147483647.",
                        TRINO_LONGTEXT);
                return BasicType.STRING_TYPE;
            case TRINO_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TRINO_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TRINO_DATETIME:
            case TRINO_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case TRINO_TINYBLOB:
            case TRINO_MEDIUMBLOB:
            case TRINO_BLOB:
            case TRINO_LONGBLOB:
            case TRINO_VARBINARY:
            case TRINO_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

                // Doesn't support yet
            case TRINO_GEOMETRY:
            case TRINO_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                if(TRINOType.contains("CHAR")){
                    return BasicType.STRING_TYPE;
                } else if (TRINOType.toUpperCase().contains("TIMESTAMP")) {
                    return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                }
                throw new RuntimeException(
                        String.format(
                                "Doesn't support TRINO type '%s' on column '%s'  yet.",
                                TRINOType, jdbcColumnName));
        }
    }
}
