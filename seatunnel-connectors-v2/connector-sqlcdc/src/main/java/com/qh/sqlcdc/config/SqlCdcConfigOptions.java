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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.ArrayList;
import java.util.List;

public class SqlCdcConfigOptions {
    private static final int DEFAULT_MAX_RETRIES = 3;

    public static final Option<String> DRIVER =
            Options.key("driver").stringType().noDefaultValue().withDescription("driver");

    public static final Option<String> DBTYPE =
            Options.key("db_type").stringType().noDefaultValue().withDescription("dbType");

    public static final Option<String> QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("query");

    public static final Option<String> URL =
            Options.key("url").stringType().noDefaultValue().withDescription("url");

    public static final Option<String> USER =
            Options.key("user").stringType().noDefaultValue().withDescription("user");

    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("password");

    public static final Option<Boolean> openDelete =
            Options.key("openDelete")
                    .booleanType()
                    .defaultValue(false).withDescription("openDelete");
    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .listType().defaultValue(new ArrayList<String>()
            ).withDescription("primary keys");

    /**
     * 每轮 pollNext 最多向下游 collect 的差异行数，避免一次吐太多导致 checkpoint 超时。
     */
    public static final Option<Integer> EMIT_BATCH_SIZE =
            Options.key("emit_batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("max rows to emit per pollNext");

    public static final String DIRECT_SINK_CONFIGS_KEY = "directSinkConfigs";
}
