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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.flink.utils.EnvironmentUtil;
import org.apache.seatunnel.core.starter.flink.utils.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class FlinkRuntimeEnvironment extends AbstractFlinkRuntimeEnvironment
        implements RuntimeEnvironment {

    private static volatile FlinkRuntimeEnvironment INSTANCE = null;

    private StreamTableEnvironment tableEnvironment;

    private FlinkRuntimeEnvironment(Config config) {
        super(config);
    }

    @Override
    public FlinkRuntimeEnvironment setConfig(Config config) {
        this.config = config;
        return this;
    }

    @Override
    public FlinkRuntimeEnvironment prepare() {
        createStreamEnvironment();
        createStreamTableEnvironment();
        if (config.hasPath("job.name")) {
            jobName = config.getString("job.name");
        }
        return this;
    }

    @Override
    public FlinkRuntimeEnvironment setJobMode(JobMode jobMode) {
        this.jobMode = jobMode;
        return this;
    }

    public StreamTableEnvironment getStreamTableEnvironment() {
        return tableEnvironment;
    }

    private void createStreamTableEnvironment() {
        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        tableEnvironment =
                StreamTableEnvironment.create(getStreamExecutionEnvironment(), environmentSettings);
        EnvironmentUtil.initTableEnvironmentConfiguration(
                this.config, tableEnvironment.getConfig().getConfiguration());
    }

    /**
     * Register a Flink DataStream as a temporary table for SQL JOIN.
     *
     * @param config plugin config (may contain field_name)
     * @param dataStream flink Row stream
     * @param name table name
     * @param isAppend true for append-only view after BATCH retract materialize
     */
    public void registerResultTable(
            Config config, DataStream<Row> dataStream, String name, Boolean isAppend) {
        StreamTableEnvironment env = this.getStreamTableEnvironment();
        if (!TableUtil.tableExists(env, name) && Boolean.TRUE.equals(isAppend)) {
            if (config.hasPath("field_name")) {
                String[] fields =
                        Arrays.stream(config.getString("field_name").split(","))
                                .map(String::trim)
                                .filter(s -> !s.isEmpty())
                                .toArray(String[]::new);
                Expression[] exprs =
                        Arrays.stream(fields).map(Expressions::$).toArray(Expression[]::new);
                env.createTemporaryView(name, env.fromDataStream(dataStream, exprs));
            } else {
                env.createTemporaryView(name, env.fromDataStream(dataStream));
            }
            return;
        }
        env.createTemporaryView(name, env.fromChangelogStream(dataStream));
    }

    public static FlinkRuntimeEnvironment getInstance(Config config) {
        if (INSTANCE == null) {
            synchronized (FlinkRuntimeEnvironment.class) {
                if (INSTANCE == null) {
                    INSTANCE = new FlinkRuntimeEnvironment(config);
                }
            }
        }
        return INSTANCE;
    }
}
