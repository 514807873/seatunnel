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

package org.apache.seatunnel.connectors.seatunnel.xjjdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.xjjdbc.config.XjJdbcSinkOptions;

import com.google.auto.service.AutoService;

/** Factory of the XjJdbc sink connector. */
@AutoService(Factory.class)
public class XjJdbcSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "XjJdbc";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        XjJdbcSinkOptions.URL, XjJdbcSinkOptions.DRIVER, XjJdbcSinkOptions.DB_TYPE)
                .optional(
                        XjJdbcSinkOptions.USER,
                        XjJdbcSinkOptions.PASSWORD,
                        XjJdbcSinkOptions.DB_SCHEMA,
                        XjJdbcSinkOptions.TABLE,
                        XjJdbcSinkOptions.BATCH_SIZE,
                        XjJdbcSinkOptions.FIELD_MAPPER,
                        XjJdbcSinkOptions.PRE_CONFIG,
                        XjJdbcSinkOptions.PRIMARY_KEYS,
                        XjJdbcSinkOptions.DB_DATASOURCE_ID,
                        XjJdbcSinkOptions.RECORD_ERROR_DATA,
                        XjJdbcSinkOptions.MAX_ERROR_NUMBER,
                        XjJdbcSinkOptions.ERROR_RECORD_URL,
                        XjJdbcSinkOptions.ERROR_RECORD_DRIVER,
                        XjJdbcSinkOptions.ERROR_RECORD_USER,
                        XjJdbcSinkOptions.ERROR_RECORD_PASSWORD,
                        XjJdbcSinkOptions.ERROR_RECORD_TABLE)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new XjJdbcSink(catalogTable, options);
    }
}
