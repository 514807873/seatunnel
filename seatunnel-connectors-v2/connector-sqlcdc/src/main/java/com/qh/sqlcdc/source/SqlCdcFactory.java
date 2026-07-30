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

package com.qh.sqlcdc.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

import static com.qh.sqlcdc.config.SqlCdcConfigOptions.DRIVER;
import static com.qh.sqlcdc.config.SqlCdcConfigOptions.PASSWORD;
import static com.qh.sqlcdc.config.SqlCdcConfigOptions.QUERY;
import static com.qh.sqlcdc.config.SqlCdcConfigOptions.URL;
import static com.qh.sqlcdc.config.SqlCdcConfigOptions.USER;

@AutoService(Factory.class)
public class SqlCdcFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "SqlCdc";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(DRIVER, QUERY, URL, USER, PASSWORD).build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SqlCdcSource.class;
    }
}
