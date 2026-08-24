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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.execution.TaskExecution;
import org.apache.seatunnel.core.starter.flink.FlinkStarter;
import org.apache.seatunnel.translation.flink.metric.FlinkJobMetricsSummary;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to execute a SeaTunnelTask.
 */
public class FlinkExecution implements TaskExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkExecution.class);

    private final FlinkRuntimeEnvironment flinkRuntimeEnvironment;
    private final PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment>
            sourcePluginExecuteProcessor;
    private final PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment>
            transformPluginExecuteProcessor;
    private final PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment>
            sinkPluginExecuteProcessor;
    private final List<URL> jarPaths;

    private JobContext jobContext;

    public FlinkExecution(Config config) {
        this.jobContext = new JobContext();
        try {
            jarPaths =
                    new ArrayList<>(
                            Collections.singletonList(
                                    new File(
                                            Common.appStarterDir()
                                                    .resolve(FlinkStarter.APP_JAR_NAME)
                                                    .toString())
                                            .toURI()
                                            .toURL()));
        } catch (MalformedURLException e) {
            throw new SeaTunnelException("load flink starter error.", e);
        }
        Config envConfig = config.getConfig("env");
        registerPlugin(envConfig);
        jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));
        int isRecordErrorData = config.getConfig("env").getInt("isRecordErrorData");
        int maxRecordNumber = config.getConfig("env").getInt("maxRecordNumber");
        jobContext.setIsRecordErrorData(isRecordErrorData);
        jobContext.setMaxRecordNumber(maxRecordNumber);
        this.sourcePluginExecuteProcessor =
                new SourceExecuteProcessor(
                        jarPaths, envConfig, config.getConfigList(Constants.SOURCE), jobContext);
        this.transformPluginExecuteProcessor =
                new TransformExecuteProcessor(
                        jarPaths,
                        envConfig,
                        TypesafeConfigUtils.getConfigList(
                                config, Constants.TRANSFORM, Collections.emptyList()),
                        jobContext);
        this.sinkPluginExecuteProcessor =
                new SinkExecuteProcessor(
                        jarPaths, envConfig, config.getConfigList(Constants.SINK), jobContext);

        this.flinkRuntimeEnvironment =
                FlinkRuntimeEnvironment.getInstance(this.registerPlugin(config, jarPaths),jobContext);

        this.sourcePluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.transformPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.sinkPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
    }

    public FlinkExecution(Config config, String jobid) {
        this.jobContext = new JobContext();
        try {
            jarPaths =
                    new ArrayList<>(
                            Collections.singletonList(
                                    new File(
                                            Common.appStarterDir()
                                                    .resolve(FlinkStarter.APP_JAR_NAME)
                                                    .toString())
                                            .toURI()
                                            .toURL()));
        } catch (MalformedURLException e) {
            throw new SeaTunnelException("load flink starter error.", e);
        }
        Config envConfig = config.getConfig("env");
        registerPlugin(envConfig);
        jobContext.setJobId(jobid);
        jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));
        int isRecordErrorData = config.getConfig("env").getInt("isRecordErrorData");
        int maxRecordNumber = config.getConfig("env").getInt("maxRecordNumber");
        jobContext.setIsRecordErrorData(isRecordErrorData);
        jobContext.setMaxRecordNumber(maxRecordNumber);
        this.sourcePluginExecuteProcessor =
                new SourceExecuteProcessor(
                        jarPaths, envConfig, config.getConfigList(Constants.SOURCE), jobContext);
        this.transformPluginExecuteProcessor =
                new TransformExecuteProcessor(
                        jarPaths,
                        envConfig,
                        TypesafeConfigUtils.getConfigList(
                                config, Constants.TRANSFORM, Collections.emptyList()),
                        jobContext);
        this.sinkPluginExecuteProcessor =
                new SinkExecuteProcessor(
                        jarPaths, envConfig, config.getConfigList(Constants.SINK), jobContext);

        this.flinkRuntimeEnvironment =
                FlinkRuntimeEnvironment.getInstance(this.registerPlugin(config, jarPaths),jobContext);

        this.sourcePluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.transformPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.sinkPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
    }
    @Override
    public void execute() throws TaskExecuteException {
        List<DataStreamTableInfo> dataStreams = new ArrayList<>();
        dataStreams = sourcePluginExecuteProcessor.execute(dataStreams);
        dataStreams = transformPluginExecuteProcessor.execute(dataStreams);
        sinkPluginExecuteProcessor.execute(dataStreams);
        LOGGER.info(
                "Flink Execution Plan: {}",
                flinkRuntimeEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
        LOGGER.info("Flink job name: {}", flinkRuntimeEnvironment.getJobName());
        if (!flinkRuntimeEnvironment.isStreaming()) {
            flinkRuntimeEnvironment
                    .getStreamExecutionEnvironment()
                    .setRuntimeMode(RuntimeExecutionMode.BATCH);
            LOGGER.info("Flink job Mode: {}", JobMode.BATCH);
        }
        try {
            StreamExecutionEnvironment env = flinkRuntimeEnvironment
                    .getStreamExecutionEnvironment();
            env.registerJobListener(new MyJobListener(this.jobContext.getJobId()));
            env.setRestartStrategy(RestartStrategies.noRestart());
            final long jobStartTime = System.currentTimeMillis();
            JobExecutionResult jobResult =
                    flinkRuntimeEnvironment
                            .getStreamExecutionEnvironment()
                            .execute(flinkRuntimeEnvironment.getJobName());
            final long jobEndTime = System.currentTimeMillis();

            final FlinkJobMetricsSummary jobMetricsSummary =
                    FlinkJobMetricsSummary.builder()
                            .jobExecutionResult(jobResult)
                            .jobStartTime(jobStartTime)
                            .jobEndTime(jobEndTime)
                            .build();

            LOGGER.info("Job finished, execution result: \n{}", jobMetricsSummary);
        } catch (Exception e) {
            throw new TaskExecuteException("Execute Flink job error", e);
        }
    }

    /**
     * Load env.jars / plugins lib / seatunnel lib onto the JM classloader for SPI discovery
     * (e.g. TableTransformFactory in seatunnel-transforms-v2).
     * pipeline.jars only keeps env.jars, plugins lib, discovered connectors, and filtered
     * runtime lib jars (transform / JDBC drivers).
     * Note: do not use local:// here — Flink 1.15 ConfigUtils.decodeListFromConfig uses
     * java.net.URL and rejects unknown protocol "local".
     */
    private void registerPlugin(Config envConfig) {
        List<Path> thirdPartyJars = new ArrayList<>();
        if (envConfig.hasPath(EnvCommonOptions.JARS.key())) {
            thirdPartyJars =
                    new ArrayList<>(
                            Common.getThirdPartyJars(
                                    envConfig.getString(EnvCommonOptions.JARS.key())));
        }
        thirdPartyJars.addAll(Common.getPluginsJarDependencies());
        List<URL> jarDependencies = pathsToUrls(thirdPartyJars);
        jarDependencies.forEach(
                url ->
                        FlinkAbstractPluginExecuteProcessor.ADD_URL_TO_CLASSLOADER.accept(
                                Thread.currentThread().getContextClassLoader(), url));
        jarPaths.addAll(jarDependencies);

        // Transform SPI and JDBC drivers live under lib; JM must see them on the classloader.
        List<URL> libJars = pathsToUrls(Common.getLibJars());
        libJars.forEach(
                url ->
                        FlinkAbstractPluginExecuteProcessor.ADD_URL_TO_CLASSLOADER.accept(
                                Thread.currentThread().getContextClassLoader(), url));
        // Ship only runtime-needed lib jars to TM; skip unused vendor SDKs.
        jarPaths.addAll(filterRuntimeLibJars(libJars));
    }

    private List<URL> pathsToUrls(List<Path> paths) {
        return paths.stream()
                .map(Path::toUri)
                .map(
                        uri -> {
                            try {
                                return uri.toURL();
                            } catch (MalformedURLException e) {
                                throw new RuntimeException("the uri of jar illegal:" + uri, e);
                            }
                        })
                .collect(Collectors.toList());
    }

    /**
     * Keep transform / JDBC driver jars for TM; drop unused vendor drivers from pipeline.jars.
     */
    private List<URL> filterRuntimeLibJars(List<URL> libJars) {
        List<URL> runtimeJars = new ArrayList<>();
        for (URL jarUrl : libJars) {
            String name = new File(jarUrl.getFile()).getName().toLowerCase();
            if (isRuntimeLibJar(name)) {
                runtimeJars.add(jarUrl);
            }
        }
        return runtimeJars;
    }

    private boolean isRuntimeLibJar(String jarName) {
        return jarName.contains("transform")
                || jarName.contains("jdbc")
                || jarName.contains("mysql-connector")
                || jarName.contains("postgresql")
                || jarName.contains("clickhouse")
                || jarName.contains("ojdbc")
                || jarName.contains("orai18n")
                || jarName.contains("dmjdbc")
                || jarName.contains("mssql")
                || jarName.contains("db2jcc")
                || jarName.contains("hexadb")
                || jarName.contains("trino")
                || jarName.contains("redshift")
                || jarName.contains("sqlite")
                || jarName.contains("tablestore")
                || jarName.contains("terajdbc")
                || jarName.startsWith("xdb")
                || jarName.contains("quality-checker")
                || jarName.contains("netty-");
    }

    private Config registerPlugin(Config config, List<URL> jars) {
        config =
                this.injectJarsToConfig(
                        config, ConfigUtil.joinPath("env", "pipeline", "jars"), jars);
        return this.injectJarsToConfig(
                config, ConfigUtil.joinPath("env", "pipeline", "classpaths"), jars);
    }

    private Config injectJarsToConfig(Config config, String path, List<URL> jars) {
        List<URL> validJars = new ArrayList<>();
        for (URL jarUrl : jars) {
            if (jarUrl != null && new File(jarUrl.getFile()).exists()) {
                validJars.add(jarUrl);
                LOGGER.info("Inject jar to config: {}", jarUrl);
            } else {
                LOGGER.warn("Remove invalid jar when inject jars into config: {}", jarUrl);
            }
        }

        Set<String> pipelineJars = new LinkedHashSet<>();
        if (config.hasPath(path)) {
            Arrays.stream(config.getString(path).split(";"))
                    .filter(uri -> uri != null && !uri.isEmpty())
                    // Skip local:// leftovers; Flink 1.15 cannot decode them via java.net.URL.
                    .filter(uri -> !uri.regionMatches(true, 0, "local:", 0, 6))
                    .forEach(pipelineJars::add);
        }
        for (URL jarUrl : validJars) {
            pipelineJars.add(jarUrl.toString());
        }

        return config.withValue(
                path, ConfigValueFactory.fromAnyRef(String.join(";", pipelineJars)));
    }
}
