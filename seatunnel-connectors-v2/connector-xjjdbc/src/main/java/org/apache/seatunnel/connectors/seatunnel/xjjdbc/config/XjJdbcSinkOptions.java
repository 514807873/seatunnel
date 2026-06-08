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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;
import java.util.Map;

/** Option definitions for the XjJdbc sink connector. */
public interface XjJdbcSinkOptions {

    Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target database jdbc url.");

    Option<String> DRIVER =
            Options.key("driver")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target database jdbc driver class name.");

    Option<String> DB_TYPE =
            Options.key("db_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Target database type, used to select the dialect "
                                    + "(mysql/oracle/pgsql/sqlserver/clickhouse/dameng/trino).");

    Option<String> USER =
            Options.key("user").stringType().noDefaultValue().withDescription("Target db user.");

    Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target db password.");

    Option<String> DB_SCHEMA =
            Options.key("dbSchema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target db schema (optional).");

    Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Target table name. Defaults to the upstream catalog table name.");

    Option<Integer> BATCH_SIZE =
            Options.key("batchSize")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Batch size for the buffered insert.");

    Option<Map<String, String>> FIELD_MAPPER =
            Options.key("field_mapper")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Field mapping from source field name to sink column name. "
                                    + "When absent, a 1:1 mapping by the upstream field names is used.");

    Option<PreConfig> PRE_CONFIG =
            Options.key("pre_config")
                    .objectType(PreConfig.class)
                    .noDefaultValue()
                    .withDescription("Pre actions executed before writing (truncate / preSql).");

    Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Logical primary keys (optional, not used for full load).");

    Option<String> DB_DATASOURCE_ID =
            Options.key("db_datasource_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Datasource id recorded together with error data.");

    Option<Boolean> RECORD_ERROR_DATA =
            Options.key("record_error_data")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to record per-row insert failures.");

    Option<Integer> MAX_ERROR_NUMBER =
            Options.key("max_error_number")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Max number of error rows to persist into the error table.");

    Option<String> ERROR_RECORD_URL =
            Options.key("error_record.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Jdbc url of the error-record database. "
                                    + "When blank, failures are only counted/logged, not persisted.");

    Option<String> ERROR_RECORD_DRIVER =
            Options.key("error_record.driver")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Jdbc driver class name of the error-record database.");

    Option<String> ERROR_RECORD_USER =
            Options.key("error_record.user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("User of the error-record database.");

    Option<String> ERROR_RECORD_PASSWORD =
            Options.key("error_record.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password of the error-record database.");

    Option<String> ERROR_RECORD_TABLE =
            Options.key("error_record.table")
                    .stringType()
                    .defaultValue("seatunnel_jobs_history_error_record")
                    .withDescription("Error-record table name.");
}
