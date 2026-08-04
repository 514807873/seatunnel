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

package com.qh.sqlcdc.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
public class SqlCdcConfig implements Serializable {
    private String driver;
    private String query;
    private String url;
    private String user;
    private String passWord;
    private String dbType;
    private Boolean openDelete;
    private List<String> primaryKeys;
    private Integer emitBatchSize;
    private List<DirectSinkConfig> directSinkConfigs = new ArrayList<>();

    public SqlCdcConfig(Config config) {
        this.driver = config.getString(SqlCdcConfigOptions.DRIVER.key());
        this.query = config.getString(SqlCdcConfigOptions.QUERY.key());
        this.url = config.getString(SqlCdcConfigOptions.URL.key());
        this.user = config.getString(SqlCdcConfigOptions.USER.key());
        this.passWord = config.getString(SqlCdcConfigOptions.PASSWORD.key());
        this.dbType = config.getString(SqlCdcConfigOptions.DBTYPE.key());
        this.openDelete = config.getBoolean(SqlCdcConfigOptions.openDelete.key());
        this.primaryKeys = config.getStringList(SqlCdcConfigOptions.PRIMARY_KEYS.key());
        if (config.hasPath(SqlCdcConfigOptions.EMIT_BATCH_SIZE.key())) {
            this.emitBatchSize = config.getInt(SqlCdcConfigOptions.EMIT_BATCH_SIZE.key());
        } else {
            this.emitBatchSize = SqlCdcConfigOptions.EMIT_BATCH_SIZE.defaultValue();
        }
        if (this.emitBatchSize == null || this.emitBatchSize <= 0) {
            this.emitBatchSize = SqlCdcConfigOptions.EMIT_BATCH_SIZE.defaultValue();
        }
        this.directSinkConfigs = parseDirectSinkConfigs(config);
    }

    private List<DirectSinkConfig> parseDirectSinkConfigs(Config config) {
        List<DirectSinkConfig> result = new ArrayList<>();
        if (!config.hasPath(SqlCdcConfigOptions.DIRECT_SINK_CONFIGS_KEY)) {
            return result;
        }
        List<? extends Config> configs =
                config.getConfigList(SqlCdcConfigOptions.DIRECT_SINK_CONFIGS_KEY);
        for (Config item : configs) {
            DirectSinkConfig sinkConfig = new DirectSinkConfig();
            sinkConfig.setUrl(item.getString("url"));
            sinkConfig.setUser(item.getString("user"));
            sinkConfig.setPassword(item.getString("password"));
            sinkConfig.setDriver(item.getString("driver"));
            sinkConfig.setDbType(item.getString("db_type"));
            sinkConfig.setQuery(item.getString("query"));
            if (item.hasPath("primary_keys")) {
                sinkConfig.setPrimaryKeys(item.getStringList("primary_keys"));
            }
            Map<String, String> fieldMapper = new LinkedHashMap<>();
            if (item.hasPath("field_mapper")) {
                ConfigObject mapperObj = item.getObject("field_mapper");
                for (Map.Entry<String, ConfigValue> entry : mapperObj.entrySet()) {
                    Object raw = entry.getValue().unwrapped();
                    if (raw != null) {
                        fieldMapper.put(entry.getKey(), String.valueOf(raw));
                    }
                }
            }
            sinkConfig.setFieldMapper(fieldMapper);
            result.add(sinkConfig);
        }
        return result;
    }

    public boolean hasDirectSinkConfigs() {
        return directSinkConfigs != null && !directSinkConfigs.isEmpty();
    }
}
