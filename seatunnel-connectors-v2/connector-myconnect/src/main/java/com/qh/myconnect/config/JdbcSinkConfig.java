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

package com.qh.myconnect.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder(builderClassName = "Builder")
public class JdbcSinkConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private JdbcConnectionConfig jdbcConnectionConfig;
    private boolean isExactlyOnce;
    private String simpleSql;
    private String database;
    private String table;
    private List<String> primaryKeys;
    private boolean supportUpsertByQueryPrimaryKeyExist;
    private String user;
    private String passWord;
    private String url;
    private PreConfig preConfig;
    private String dbType;
    private Map<String, String> fieldMapper;
    private Map<String, String> codeMapper;
    private Map<String, String> decodeMapper;
    private String driver;
    private String dbSchema;
    private String dbDatasourceId;
    private boolean openQuality;
    private List<QualityFieldRule> qualityFieldRule;
    private int batchSize;

    public static JdbcSinkConfig of(ReadonlyConfig config) {
        JdbcSinkConfig.Builder builder = JdbcSinkConfig.builder();
        builder.jdbcConnectionConfig(JdbcConnectionConfig.of(config));
        builder.isExactlyOnce(config.get(JdbcOptions.IS_EXACTLY_ONCE));
        builder.user(config.get(JdbcOptions.USER));
        builder.driver(config.get(JdbcOptions.DRIVER));
        builder.passWord(config.get(JdbcOptions.PASSWORD));
        builder.url(config.get(JdbcOptions.URL));
        config.getOptional(JdbcOptions.PRIMARY_KEYS).ifPresent(builder::primaryKeys);
        config.getOptional(JdbcOptions.DATABASE).ifPresent(builder::database);
        config.getOptional(JdbcOptions.TABLE).ifPresent(builder::table);
        config.getOptional(JdbcOptions.SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST)
                .ifPresent(builder::supportUpsertByQueryPrimaryKeyExist);
        config.getOptional(JdbcOptions.BATCH_SIZE).ifPresentOrElse(builder::batchSize, () -> builder.batchSize(1000));
        config.getOptional(JdbcOptions.QUERY).ifPresent(builder::simpleSql);
        config.getOptional(JdbcOptions.PRE_CONFIG).ifPresent(builder::preConfig);
        config.getOptional(JdbcOptions.DB_TYPE).ifPresent(builder::dbType);
        config.getOptional(JdbcOptions.dbSchema).ifPresent(builder::dbSchema);
        config.getOptional(JdbcOptions.FIELD_MAPPER).ifPresent(builder::fieldMapper);
        config.getOptional(JdbcOptions.CODE_MAPPER).ifPresent(builder::codeMapper);
        config.getOptional(JdbcOptions.DECODE_MAPPER).ifPresent(builder::decodeMapper);
        config.getOptional(JdbcOptions.DB_DATASOURCE_ID).ifPresent(builder::dbDatasourceId);
        config.getOptional(JdbcOptions.openQuality).ifPresent(builder::openQuality);
        config.getOptional(JdbcOptions.qualityFieldRule).ifPresent(builder::qualityFieldRule);
        return builder.build();
    }
}
