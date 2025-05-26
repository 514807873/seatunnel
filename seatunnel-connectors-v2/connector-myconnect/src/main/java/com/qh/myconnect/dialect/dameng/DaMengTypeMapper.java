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

package com.qh.myconnect.dialect.dameng;

import com.qh.myconnect.dialect.JdbcConnectorException;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class DaMengTypeMapper implements JdbcDialectTypeMapper {

    // ============================data types=====================

    static final String DAMENG_UNKNOWN = "UNKNOWN";

    // -------------------------number----------------------------
    static final String DAMENG_BINARY_DOUBLE = "BINARY_DOUBLE";
    static final String DAMENG_BINARY_FLOAT = "BINARY_FLOAT";
    static final String DAMENG_NUMBER = "NUMBER";
    static final String DAMENG_FLOAT = "FLOAT";
    static final String DAMENG_REAL = "REAL";
    static final String DAMENG_INTEGER = "INTEGER";

    // -------------------------string----------------------------
    static final String DAMENG_CHAR = "CHAR";
    static final String DAMENG_VARCHAR = "VARCHAR";
    static final String DAMENG_VARCHAR2 = "VARCHAR2";
    static final String DAMENG_NCHAR = "NCHAR";
    static final String DAMENG_NVARCHAR2 = "NVARCHAR2";
    static final String DAMENG_LONG = "LONG";
    static final String DAMENG_ROWID = "ROWID";
    static final String DAMENG_CLOB = "CLOB";
    static final String DAMENG_TEXT = "TEXT";
    static final String DAMENG_NCLOB = "NCLOB";

    // ------------------------------time-------------------------
    static final String DAMENG_DATE = "DATE";
    static final String DAMENG_TIMESTAMP = "TIMESTAMP";
    static final String DAMENG_TIMESTAMP_WITH_LOCAL_TIME_ZONE = "TIMESTAMP WITH LOCAL TIME ZONE";

    // ------------------------------blob-------------------------
    static final String DAMENG_BLOB = "BLOB";
    static final String DAMENG_BFILE = "BFILE";
    static final String DAMENG_RAW = "RAW";
    static final String DAMENG_LONG_RAW = "LONG RAW";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String DAMENGType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (DAMENGType) {
            case DAMENG_INTEGER:
                return BasicType.INT_TYPE;
            case DAMENG_FLOAT:
                // The float type will be converted to DecimalType(10, -127),
                // which will lose precision in the spark engine
                return new DecimalType(38, 18);
            case DAMENG_NUMBER:
                if (scale == 0) {
                    if (precision <= 9) {
                        return BasicType.INT_TYPE;
                    }
                    if (precision <= 18) {
                        return BasicType.LONG_TYPE;
                    }
                }
                return new DecimalType(38, 18);
            case DAMENG_BINARY_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DAMENG_BINARY_FLOAT:
            case DAMENG_REAL:
                return BasicType.FLOAT_TYPE;
            case DAMENG_CHAR:
            case DAMENG_NCHAR:
            case DAMENG_NVARCHAR2:
            case DAMENG_VARCHAR:
            case DAMENG_VARCHAR2:
            case DAMENG_LONG:
            case DAMENG_ROWID:
            case DAMENG_NCLOB:
            case DAMENG_CLOB:
            case DAMENG_TEXT:
                return BasicType.STRING_TYPE;
            case DAMENG_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case DAMENG_TIMESTAMP:
            case DAMENG_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DAMENG_BLOB:
            case DAMENG_RAW:
            case DAMENG_LONG_RAW:
            case DAMENG_BFILE:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            case DAMENG_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR,
                        String.format(
                                "Doesn't support DAMENG type '%s' on column '%s'  yet.",
                                DAMENGType, jdbcColumnName));
        }
    }
}
