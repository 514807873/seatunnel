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

package org.apache.seatunnel.connectors.seatunnel.xjjdbc.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Parsed configuration of the XjJdbc sink. */
@Data
public class XjJdbcSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String url;
    private String driver;
    private String dbType;
    private String user;
    private String passWord;
    private String dbSchema;
    private String table;
    private int batchSize;
    private Map<String, String> fieldMapper;
    private List<String> primaryKeys;
    private PreConfig preConfig;

    private String dbDatasourceId;
    private boolean recordErrorData;
    private int maxErrorNumber;
    private String errorRecordUrl;
    private String errorRecordDriver;
    private String errorRecordUser;
    private String errorRecordPassword;
    private String errorRecordTable;

    public static XjJdbcSinkConfig of(ReadonlyConfig config) {
        XjJdbcSinkConfig sinkConfig = new XjJdbcSinkConfig();
        sinkConfig.setUrl(config.get(XjJdbcSinkOptions.URL));
        sinkConfig.setDriver(config.get(XjJdbcSinkOptions.DRIVER));
        sinkConfig.setDbType(config.get(XjJdbcSinkOptions.DB_TYPE));
        config.getOptional(XjJdbcSinkOptions.USER).ifPresent(sinkConfig::setUser);
        config.getOptional(XjJdbcSinkOptions.PASSWORD).ifPresent(sinkConfig::setPassWord);
        config.getOptional(XjJdbcSinkOptions.DB_SCHEMA).ifPresent(sinkConfig::setDbSchema);
        config.getOptional(XjJdbcSinkOptions.TABLE).ifPresent(sinkConfig::setTable);
        sinkConfig.setBatchSize(config.get(XjJdbcSinkOptions.BATCH_SIZE));
        config.getOptional(XjJdbcSinkOptions.FIELD_MAPPER).ifPresent(sinkConfig::setFieldMapper);
        config.getOptional(XjJdbcSinkOptions.PRIMARY_KEYS).ifPresent(sinkConfig::setPrimaryKeys);

        PreConfig preConfig = config.get(XjJdbcSinkOptions.PRE_CONFIG);
        sinkConfig.setPreConfig(preConfig == null ? new PreConfig() : preConfig);

        config.getOptional(XjJdbcSinkOptions.DB_DATASOURCE_ID)
                .ifPresent(sinkConfig::setDbDatasourceId);
        sinkConfig.setRecordErrorData(config.get(XjJdbcSinkOptions.RECORD_ERROR_DATA));
        sinkConfig.setMaxErrorNumber(config.get(XjJdbcSinkOptions.MAX_ERROR_NUMBER));
        config.getOptional(XjJdbcSinkOptions.ERROR_RECORD_URL)
                .ifPresent(sinkConfig::setErrorRecordUrl);
        config.getOptional(XjJdbcSinkOptions.ERROR_RECORD_DRIVER)
                .ifPresent(sinkConfig::setErrorRecordDriver);
        config.getOptional(XjJdbcSinkOptions.ERROR_RECORD_USER)
                .ifPresent(sinkConfig::setErrorRecordUser);
        config.getOptional(XjJdbcSinkOptions.ERROR_RECORD_PASSWORD)
                .ifPresent(sinkConfig::setErrorRecordPassword);
        sinkConfig.setErrorRecordTable(config.get(XjJdbcSinkOptions.ERROR_RECORD_TABLE));
        return sinkConfig;
    }
}
