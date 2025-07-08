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

//    public static Option<List<String>> PRIMARY_KEYS =
//            Options.key("primary_keys").listType().noDefaultValue().withDescription("primary keys");

    //    public static Option<Boolean> directCompare =
    //            Options.key("directCompare")
    //                    .booleanType()
    //                    .noDefaultValue()
    //                    .withDescription("directCompare");
    //    public static Option<Boolean> recordOperation =
    //            Options.key("recordOperation")
    //                    .booleanType()
    //                    .noDefaultValue()
    //                    .withDescription("是否记录操作类型与时间戳");
    //    public static Option<JSONObject> directSinkConfig =
    //            Options.key("directSinkConfig")
    //                    .objectType(JSONObject.class)
    //                    .noDefaultValue()
    //                    .withDescription("directSinkConfig");
}
