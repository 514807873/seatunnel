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

package com.qh.myconnect.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import org.apache.commons.collections4.CollectionUtils;

import com.google.auto.service.AutoService;
import com.qh.myconnect.config.JdbcOptions;
import com.qh.myconnect.config.JdbcSinkConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@AutoService(Factory.class)
public class MySinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "XjJdbc";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(JdbcOptions.URL, JdbcOptions.DRIVER, JdbcOptions.DB_TYPE)
                .optional(
                        JdbcOptions.USER,
                        JdbcOptions.PASSWORD,
                        JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC,
                        JdbcOptions.BATCH_SIZE,
                        JdbcOptions.BATCH_INTERVAL_MS,
                        JdbcOptions.IS_EXACTLY_ONCE,
                        JdbcOptions.GENERATE_SINK_SQL,
                        JdbcOptions.AUTO_COMMIT,
                        JdbcOptions.SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST,
                        JdbcOptions.FIELD_MAPPER,
                        JdbcOptions.CODE_MAPPER,
                        JdbcOptions.DECODE_MAPPER,
                        JdbcOptions.PRE_CONFIG,
                        JdbcOptions.PRIMARY_KEYS)
                .conditional(
                        JdbcOptions.IS_EXACTLY_ONCE,
                        true,
                        JdbcOptions.XA_DATA_SOURCE_CLASS_NAME,
                        JdbcOptions.MAX_COMMIT_ATTEMPTS,
                        JdbcOptions.TRANSACTION_TIMEOUT_SEC)
                .conditional(JdbcOptions.IS_EXACTLY_ONCE, false, JdbcOptions.MAX_RETRIES)
                //                .conditional(GENERATE_SINK_SQL, true, DATABASE)
                //                .conditional(GENERATE_SINK_SQL, false, QUERY)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        Optional<String> optionalTable = config.getOptional(JdbcOptions.TABLE);
        if (!optionalTable.isPresent()) {
            Map<String, String> map = config.toMap();
            map.put(JdbcOptions.TABLE.key(), catalogTable.getTableId().getTableName());

            PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
            if (primaryKey != null && !CollectionUtils.isEmpty(primaryKey.getColumnNames())) {
                map.put(
                        JdbcOptions.PRIMARY_KEYS.key(),
                        String.join(",", primaryKey.getColumnNames()));
            }
            config = ReadonlyConfig.fromMap(new HashMap<>(map));
        }
        final ReadonlyConfig options = config;
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);
        return () ->
                new MySink(
                        context.getCatalogTable().getTableSchema().toPhysicalRowDataType(),
                        options,
                        sinkConfig);
    }
}
