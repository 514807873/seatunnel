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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.GroupConcatQueryResult;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.flink.utils.FlinkTableRowBridge;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;

@SuppressWarnings("unchecked,rawtypes")
public class TransformExecuteProcessor
        extends FlinkAbstractPluginExecuteProcessor<TableTransformFactory> {

    protected TransformExecuteProcessor(
            List<URL> jarPaths,
            Config envConfig,
            List<? extends Config> pluginConfigs,
            JobContext jobContext) {
        super(jarPaths, envConfig, pluginConfigs, jobContext);
    }

    @Override
    protected List<TableTransformFactory> initializePlugins(
            List<URL> jarPaths, List<? extends Config> pluginConfigs) {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();
        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableTransformFactory.class, ADD_URL_TO_CLASSLOADER);
        return pluginConfigs.stream()
                .map(
                        transformConfig -> {
                            jarPaths.addAll(
                                    transformPluginDiscovery.getPluginJarPaths(
                                            Lists.newArrayList(
                                                    PluginIdentifier.of(
                                                            EngineType.SEATUNNEL.getEngine(),
                                                            PluginType.TRANSFORM.getType(),
                                                            transformConfig.getString(
                                                                    PLUGIN_NAME.key())))));
                            return Optional.of(
                                    (TableTransformFactory)
                                            factoryDiscovery.createPluginInstance(
                                                    PluginIdentifier.of(
                                                            EngineType.SEATUNNEL.getEngine(),
                                                            PluginType.TRANSFORM.getType(),
                                                            transformConfig.getString(
                                                                    PLUGIN_NAME.key()))));
                        })
                .distinct()
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    @Override
    public List<DataStreamTableInfo> execute(List<DataStreamTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        if (plugins.isEmpty()) {
            return upstreamDataStreams;
        }
        DataStreamTableInfo input = upstreamDataStreams.get(0);
        Map<String, DataStreamTableInfo> outputTables =
                upstreamDataStreams.stream()
                        .collect(
                                Collectors.toMap(
                                        DataStreamTableInfo::getTableName,
                                        e -> e,
                                        (a, b) -> b,
                                        LinkedHashMap::new));

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (int i = 0; i < plugins.size(); i++) {
            try {
                Config pluginConfig = pluginConfigs.get(i);
                DataStreamTableInfo stream =
                        fromSourceTable(pluginConfig, new ArrayList<>(outputTables.values()))
                                .orElse(input);
                TableTransformFactory factory = plugins.get(i);
                TableTransformFactoryContext context =
                        new TableTransformFactoryContext(
                                stream.getCatalogTables(),
                                ReadonlyConfig.fromConfig(pluginConfig),
                                classLoader);
                ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
                SeaTunnelTransform transform = factory.createTransform(context).createTransform();

                transform.setJobContext(jobContext);
                String pluginOutputIdentifier =
                        ReadonlyConfig.fromConfig(pluginConfig).get(PLUGIN_OUTPUT);
                DataStream<SeaTunnelRow> inputStream;
                if (isFlinkSqlEngine(transform, pluginConfig)) {
                    boolean batchJoin = JobMode.BATCH.equals(jobContext.getJobMode());
                    StreamTableEnvironment tableEnv =
                            flinkRuntimeEnvironment.getStreamTableEnvironment();
                    Table joinTable = tableEnv.sqlQuery(pluginConfig.getString("query"));
                    DataStream<Row> joinedStream = joinStream(pluginConfig, joinTable);
                    Config configWithFields =
                            pluginConfig.withValue(
                                    "field_name",
                                    org.apache.seatunnel.shade.com.typesafe.config
                                            .ConfigValueFactory.fromAnyRef(
                                            String.join(
                                                    ",", joinTable.getSchema().getFieldNames())));
                    // BATCH join result registered as append to avoid changelog reconversion
                    registerResultTable(configWithFields, joinedStream, batchJoin);
                    inputStream = FlinkTableRowBridge.toSeaTunnelRowStream(joinedStream);
                } else {
                    inputStream = flinkTransform(transform, stream.getDataStream(), pluginConfig);
                    registerSeaTunnelResultTable(
                            pluginConfig, inputStream, transform.getProducedCatalogTables());
                }
                outputTables.put(
                        pluginOutputIdentifier,
                        new DataStreamTableInfo(
                                inputStream,
                                transform.getProducedCatalogTables(),
                                pluginOutputIdentifier));
            } catch (Exception e) {
                throw new TaskExecuteException(
                        String.format(
                                "SeaTunnel transform task: %s execute error",
                                plugins.get(i).factoryIdentifier()),
                        e);
            }
        }
        return new ArrayList<>(outputTables.values());
    }

    private boolean isFlinkSqlEngine(SeaTunnelTransform transform, Config pluginConfig) {
        return "Sql".equalsIgnoreCase(transform.getPluginName())
                && pluginConfig.hasPath("engine")
                && "FLINK".equalsIgnoreCase(pluginConfig.getString("engine"));
    }

    /**
     * Execute Flink Table SQL (typically multi-table JOIN) and materialize BATCH retract stream.
     */
    protected DataStream<Row> joinStream(Config pluginConfig, Table joinTable) {
        StreamTableEnvironment tableEnv = flinkRuntimeEnvironment.getStreamTableEnvironment();
        TypeInformation<Row> typeInfo = joinTable.getSchema().toRowType();
        DataStream<Tuple2<Boolean, Row>> retractStream =
                tableEnv.toRetractStream(joinTable, typeInfo);

        if (JobMode.BATCH.equals(jobContext.getJobMode())) {
            return retractStream
                    .transform(
                            "retract-batch-materialize",
                            typeInfo,
                            new RetractBatchMaterializeOperator())
                    .name("retract-batch-materialize");
        }
        return retractStream.filter(row -> row.f0).map(row -> row.f1).returns(typeInfo);
    }

    /**
     * BATCH only: consume retract stream, emit final row per left-table business key at endInput.
     * LEFT JOIN common sequence: +I(null right) -> -D(null right) -> +I(matched), keep last +I.
     */
    private static class RetractBatchMaterializeOperator extends AbstractStreamOperator<Row>
            implements OneInputStreamOperator<Tuple2<Boolean, Row>, Row>, BoundedOneInput {

        private final Map<String, Row> latestByLeftKey = new HashMap<>();

        @Override
        public void processElement(StreamRecord<Tuple2<Boolean, Row>> element) {
            Tuple2<Boolean, Row> value = element.getValue();
            Row row = Row.copy(value.f1);
            String key = leftKey(row);
            if (Boolean.TRUE.equals(value.f0)) {
                latestByLeftKey.put(key, row);
            } else {
                Row current = latestByLeftKey.get(key);
                if (current != null && Objects.equals(rowKey(current), rowKey(row))) {
                    latestByLeftKey.remove(key);
                }
            }
        }

        @Override
        public void endInput() {
            for (Row row : latestByLeftKey.values()) {
                output.collect(new StreamRecord<>(row));
            }
            latestByLeftKey.clear();
        }

        /** Left-table business key: field[0] + field[2], matching historical 2.3.5 behavior. */
        private static String leftKey(Row row) {
            return Objects.toString(row.getField(0), "")
                    + '\u0000'
                    + Objects.toString(row.getField(2), "");
        }

        private static String rowKey(Row row) {
            StringBuilder sb = new StringBuilder();
            int arity = row.getArity();
            for (int i = 0; i < arity; i++) {
                if (i > 0) {
                    sb.append('\u0001');
                }
                sb.append(Objects.toString(row.getField(i), ""));
            }
            return sb.toString();
        }
    }

    protected DataStream<SeaTunnelRow> flinkTransform(
            SeaTunnelTransform transform, DataStream<SeaTunnelRow> stream, Config pluginConfig) {
        if ("GroupConcat".equalsIgnoreCase(transform.getPluginName())) {
            org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<SeaTunnelRow>
                    operator =
                            stream.transform(
                                            String.format(
                                                    "%s-Transform", transform.getPluginName()),
                                            TypeInformation.of(SeaTunnelRow.class),
                                            new GroupConcatBoundedOperator(transform))
                                    .name(String.format("%s-Transform", transform.getPluginName()));
            // GroupConcat needs global aggregation over one SQLite store
            operator.setParallelism(1);
            return operator;
        }
        org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<SeaTunnelRow>
                operator =
                        stream.flatMap(
                                        new TransformRichFlatMap(transform),
                                        TypeInformation.of(SeaTunnelRow.class))
                                .name(String.format("%s-Transform", transform.getPluginName()));
        if (pluginConfig.hasPath("parallelism")) {
            operator.setParallelism(pluginConfig.getInt("parallelism"));
        }
        return operator;
    }

    /**
     * Batch-friendly GroupConcat path: insert rows into SQLite during processElement, then run
     * GROUP_CONCAT query when input ends.
     */
    private static class GroupConcatBoundedOperator extends AbstractStreamOperator<SeaTunnelRow>
            implements OneInputStreamOperator<SeaTunnelRow, SeaTunnelRow>, BoundedOneInput {

        private final SeaTunnelTransform transform;

        private GroupConcatBoundedOperator(SeaTunnelTransform transform) {
            this.transform = transform;
        }

        @Override
        public void open() throws Exception {
            super.open();
            transform.setSubtaskIndex(getRuntimeContext().getIndexOfThisSubtask());
            transform.open();
        }

        @Override
        public void processElement(StreamRecord<SeaTunnelRow> element) {
            transform.mapList(element.getValue());
        }

        @Override
        public void endInput() throws Exception {
            try (GroupConcatQueryResult queryResult = transform.executeGroupConcatQuery()) {
                ResultSet resultSet = queryResult.getResultSet();
                int columnCount = resultSet.getMetaData().getColumnCount();
                while (resultSet.next()) {
                    Object[] newFields = new Object[columnCount];
                    for (int i = 1; i <= columnCount; i++) {
                        newFields[i - 1] = resultSet.getObject(i);
                    }
                    SeaTunnelRow newRow = new SeaTunnelRow(newFields);
                    newRow.setRowKind(RowKind.INSERT);
                    output.collect(new StreamRecord<>(newRow));
                }
            }
        }

        @Override
        public void close() throws Exception {
            transform.close();
            super.close();
        }
    }

    public static class TransformRichFlatMap
            extends org.apache.flink.api.common.functions.RichFlatMapFunction<
                    SeaTunnelRow, SeaTunnelRow> {

        private final SeaTunnelTransform transform;

        public TransformRichFlatMap(SeaTunnelTransform transform) {
            this.transform = transform;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            transform.setSubtaskIndex(getRuntimeContext().getIndexOfThisSubtask());
            transform.open();
        }

        @Override
        public void flatMap(SeaTunnelRow row, Collector<SeaTunnelRow> collector) {
            List<SeaTunnelRow> mappedList = transform.mapList(row);
            if (mappedList != null) {
                for (SeaTunnelRow rowResult : mappedList) {
                    if (rowResult != null) {
                        collector.collect(rowResult);
                    }
                }
                return;
            }
            if (transform instanceof SeaTunnelFlatMapTransform) {
                List<SeaTunnelRow> rows =
                        ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(row);
                if (CollectionUtils.isNotEmpty(rows)) {
                    for (SeaTunnelRow rowResult : rows) {
                        if (rowResult != null) {
                            collector.collect(rowResult);
                        }
                    }
                }
                return;
            }
            SeaTunnelRow result = ((SeaTunnelMapTransform<SeaTunnelRow>) transform).map(row);
            if (result != null) {
                collector.collect(result);
            }
        }

        @Override
        public void close() {
            transform.close();
        }
    }

    /** @deprecated kept for compatibility with older call sites */
    public static class ArrayFlatMap implements FlatMapFunction<SeaTunnelRow, SeaTunnelRow> {

        private SeaTunnelTransform transform;

        public ArrayFlatMap(SeaTunnelTransform transform) {
            this.transform = transform;
        }

        @Override
        public void flatMap(SeaTunnelRow row, Collector<SeaTunnelRow> collector) {
            List<SeaTunnelRow> rows =
                    ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(row);
            if (CollectionUtils.isNotEmpty(rows)) {
                for (SeaTunnelRow rowResult : rows) {
                    collector.collect(rowResult);
                }
            }
        }
    }
}
