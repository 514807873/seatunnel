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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;
import org.apache.seatunnel.core.starter.flink.utils.FlinkTableRowBridge;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_INPUT;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;

public abstract class FlinkAbstractPluginExecuteProcessor<T>
        implements PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment> {

    protected static final BiConsumer<ClassLoader, List<URL>> ADD_URL_TO_CLASSLOADER =
            (classLoader, urls) -> {
                if (classLoader.getClass().getName().endsWith("SafetyNetWrapperClassLoader")) {
                    URLClassLoader c =
                            (URLClassLoader) ReflectionUtils.getField(classLoader, "inner").get();
                    urls.forEach(url -> ReflectionUtils.invoke(c, "addURL", url));
                } else if (classLoader instanceof URLClassLoader) {
                    urls.forEach(url -> ReflectionUtils.invoke(classLoader, "addURL", url));
                } else {
                    try {
                        // In Java 8, AppClassLoader is a subclass of URLClassLoader, so classLoader
                        // instanceof URLClassLoader will return true. However, in Java 11, due to
                        // the introduction of the modular system, AppClassLoader is no longer a
                        // subclass of URLClassLoader, and this check will return false. To be
                        // compatible with both Java 8 and Java 11, we can use reflection to
                        // dynamically call the addURL method of URLClassLoader.
                        Optional<Method> method =
                                ReflectionUtils.getDeclaredMethod(
                                        URLClassLoader.class, "addURL", URL.class);
                        if (method.isPresent()) {
                            for (URL url : urls) {
                                method.get().invoke(classLoader, url);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Unsupported classloader: " + classLoader.getClass().getName(), e);
                    }
                }
            };

    protected FlinkRuntimeEnvironment flinkRuntimeEnvironment;
    protected final List<? extends Config> pluginConfigs;
    protected JobContext jobContext;
    protected final List<T> plugins;
    protected final Config envConfig;
    protected final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    protected final Map<String, Boolean> isAppendStreamMap = new HashMap<>();

    protected FlinkAbstractPluginExecuteProcessor(
            List<URL> jarPaths,
            Config envConfig,
            List<? extends Config> pluginConfigs,
            JobContext jobContext) {
        this.pluginConfigs = pluginConfigs;
        this.jobContext = jobContext;
        this.plugins = initializePlugins(jarPaths, pluginConfigs);
        this.envConfig = envConfig;
    }

    @Override
    public void setRuntimeEnvironment(FlinkRuntimeEnvironment flinkRuntimeEnvironment) {
        this.flinkRuntimeEnvironment = flinkRuntimeEnvironment;
    }

    protected Optional<DataStreamTableInfo> fromSourceTable(
            Config pluginConfig, List<DataStreamTableInfo> upstreamDataStreams) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);

        if (readonlyConfig.getOptional(PLUGIN_INPUT).isPresent()) {
            List<String> pluginInputIdentifiers = readonlyConfig.get(PLUGIN_INPUT);
            if (pluginInputIdentifiers.size() == 1) {
                String tableName = pluginInputIdentifiers.get(0);
                DataStreamTableInfo dataStreamTableInfo =
                        upstreamDataStreams.stream()
                                .filter(info -> tableName.equals(info.getTableName()))
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new SeaTunnelException(
                                                        String.format(
                                                                "table %s not found", tableName)));
                return Optional.of(
                        new DataStreamTableInfo(
                                dataStreamTableInfo.getDataStream(),
                                dataStreamTableInfo.getCatalogTables(),
                                tableName));
            }
            // Multi-table input for Flink SQL JOIN: merge catalog tables, keep first stream
            List<CatalogTable> catalogTables = new ArrayList<>();
            DataStream<SeaTunnelRow> firstStream = null;
            for (String tableName : pluginInputIdentifiers) {
                DataStreamTableInfo info =
                        upstreamDataStreams.stream()
                                .filter(item -> tableName.equals(item.getTableName()))
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new SeaTunnelException(
                                                        String.format(
                                                                "table %s not found", tableName)));
                catalogTables.addAll(info.getCatalogTables());
                if (firstStream == null) {
                    firstStream = info.getDataStream();
                }
            }
            return Optional.of(
                    new DataStreamTableInfo(
                            firstStream, catalogTables, pluginInputIdentifiers.get(0)));
        }
        return Optional.empty();
    }

    protected void registerSeaTunnelResultTable(
            Config pluginConfig,
            DataStream<SeaTunnelRow> dataStream,
            List<CatalogTable> catalogTables) {
        registerSeaTunnelResultTable(pluginConfig, dataStream, catalogTables, true);
    }

    protected void registerSeaTunnelResultTable(
            Config pluginConfig,
            DataStream<SeaTunnelRow> dataStream,
            List<CatalogTable> catalogTables,
            boolean isAppend) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        if (!readonlyConfig.getOptional(PLUGIN_OUTPUT).isPresent()) {
            return;
        }
        String resultTable = readonlyConfig.get(PLUGIN_OUTPUT);
        SeaTunnelRowType rowType = catalogTables.get(0).getSeaTunnelRowType();
        DataStream<Row> rowStream = FlinkTableRowBridge.toFlinkRowStream(dataStream, rowType);
        Config configWithFields =
                pluginConfig.withValue(
                        "field_name",
                        ConfigValueFactory.fromAnyRef(
                                Arrays.stream(rowType.getFieldNames())
                                        .collect(Collectors.joining(","))));
        flinkRuntimeEnvironment.registerResultTable(
                configWithFields, rowStream, resultTable, isAppend);
        isAppendStreamMap.put(resultTable, isAppend);
    }

    protected void registerResultTable(
            Config pluginConfig, DataStream<Row> dataStream, boolean isAppend) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        if (!readonlyConfig.getOptional(PLUGIN_OUTPUT).isPresent()) {
            return;
        }
        String resultTable = readonlyConfig.get(PLUGIN_OUTPUT);
        flinkRuntimeEnvironment.registerResultTable(
                pluginConfig, dataStream, resultTable, isAppend);
        isAppendStreamMap.put(resultTable, isAppend);
    }

    protected abstract List<T> initializePlugins(
            List<URL> jarPaths, List<? extends Config> pluginConfigs);
}
