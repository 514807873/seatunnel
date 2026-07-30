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

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.GroupConcatQueryResult;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;
import org.apache.seatunnel.translation.flink.serialization.FlinkRowConverter;
import org.apache.seatunnel.translation.flink.utils.TypeConverterUtils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import java.net.URL;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;

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

        return pluginConfigs.stream()
                .map(
                        transformConfig ->
                                PluginUtil.createTransformFactory(
                                        transformPluginDiscovery, transformConfig, jarPaths))
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<DataStreamTableInfo> execute(List<DataStreamTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        if (plugins.isEmpty()) {
            return upstreamDataStreams;
        }
        List<DataStreamTableInfo> input = upstreamDataStreams;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (int i = 0; i < plugins.size(); i++) {
            try {
                Config pluginConfig = pluginConfigs.get(i);
                List<DataStreamTableInfo> streamList = fromSourceTable(pluginConfig, upstreamDataStreams).orElse(input);
                TableTransformFactory factory = plugins.get(i);
                TableTransformFactoryContext context =
                        new TableTransformFactoryContext(
                                streamList.stream().map(DataStreamTableInfo::getCatalogTable).collect(Collectors.toList()),
                                ReadonlyConfig.fromConfig(pluginConfig),
                                classLoader);
                ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
                SeaTunnelTransform transform = factory.createTransform(context).createTransform();

                List<SeaTunnelRowType> sourceType = streamList.stream().map(stream -> stream.getCatalogTable().getSeaTunnelRowType()).collect(Collectors.toList());
                transform.setJobContext(jobContext);
                // TODO: 需要后期优化代码的判断
                DataStream<Row> inputStream;
                if ("SQL".equalsIgnoreCase(transform.getPluginName()) && pluginConfig.hasPath("engine")
                    && "FLINK".equalsIgnoreCase(pluginConfig.getString("engine"))) {
                    boolean batchJoin = JobMode.BATCH.equals(jobContext.getJobMode());
                    inputStream = joinStream(pluginConfig);
                    // BATCH 关联结果按 append 注册，避免下游再按 changelog 二次转换
                    registerResultTable(pluginConfig, inputStream, batchJoin);
                    upstreamDataStreams.add(
                            new DataStreamTableInfo(
                                    inputStream,
                                    transform.getProducedCatalogTable(),
                                    pluginConfig.hasPath(RESULT_TABLE_NAME.key())
                                            ? pluginConfig.getString(RESULT_TABLE_NAME.key())
                                            : null));
                }
                else {
                    // TODO: 暂时取第一个元素
                    inputStream = flinkTransform(sourceType.get(0), transform, streamList.get(0).getDataStream());
                    if (pluginConfig.hasPath(CommonOptions.PARALLELISM.key())) {
                        int parallelism = pluginConfig.getInt(CommonOptions.PARALLELISM.key());
                        ((SingleOutputStreamOperator<Row>) inputStream).setParallelism(parallelism);
                    }
                    registerResultTable(pluginConfig, inputStream, false);
                    upstreamDataStreams.add(
                            new DataStreamTableInfo(
                                    inputStream,
                                    transform.getProducedCatalogTable(),
                                    pluginConfig.hasPath(RESULT_TABLE_NAME.key())
                                            ? pluginConfig.getString(RESULT_TABLE_NAME.key())
                                            : null));
                }
            } catch (Exception e) {
                throw new TaskExecuteException(
                        String.format(
                                "SeaTunnel transform task: %s execute error",
                                plugins.get(i).factoryIdentifier()),
                        e);
            }
        }
        return upstreamDataStreams;
    }

    protected DataStream<Row> flinkTransform(
            SeaTunnelRowType sourceType, SeaTunnelTransform transform, DataStream<Row> stream) {
        if (transform.getPluginName().equalsIgnoreCase("GroupConcat")) {
            return flinkTransformGroupConcat(sourceType, transform, stream);
        }
        TypeInformation rowTypeInfo =
                TypeConverterUtils.convert(
                        transform.getProducedCatalogTable().getSeaTunnelRowType());
        FlinkRowConverter transformInputRowConverter = new FlinkRowConverter(sourceType);
        FlinkRowConverter transformOutputRowConverter =
                new FlinkRowConverter(transform.getProducedCatalogTable().getSeaTunnelRowType());
        DataStream<Row> output =
                stream.flatMap(
                        new TransformFlatMapFunction(
                                transform, transformInputRowConverter, transformOutputRowConverter),
                        rowTypeInfo);
        return output;
    }

    private static class TransformFlatMapFunction extends RichFlatMapFunction<Row, Row> {
        private final SeaTunnelTransform transform;
        private final FlinkRowConverter transformInputRowConverter;
        private final FlinkRowConverter transformOutputRowConverter;

        private TransformFlatMapFunction(
                SeaTunnelTransform transform,
                FlinkRowConverter transformInputRowConverter,
                FlinkRowConverter transformOutputRowConverter) {
            this.transform = transform;
            this.transformInputRowConverter = transformInputRowConverter;
            this.transformOutputRowConverter = transformOutputRowConverter;
        }

        @Override
        public void open(Configuration parameters) {
            transform.setSubtaskIndex(getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void flatMap(Row value, Collector<Row> out) throws Exception {
            SeaTunnelRow seaTunnelRow = transformInputRowConverter.reconvert(value);
            if (transform.getPluginName().equalsIgnoreCase("http_transform")
                    || transform.getPluginName().equalsIgnoreCase("SplitColumnValue")) {
                List<SeaTunnelRow> list = transform.mapList(seaTunnelRow);
                if (!list.isEmpty()) {
                    for (SeaTunnelRow dataRow : list) {
                        Row copy = transformOutputRowConverter.convert(dataRow);
                        out.collect(copy);
                    }
                }
            } else {
                SeaTunnelRow dataRow = (SeaTunnelRow) transform.map(seaTunnelRow);
                if (dataRow != null) {
                    Row copy = transformOutputRowConverter.convert(dataRow);
                    out.collect(copy);
                }
            }
        }
    }

    protected DataStream<Row> flinkTransformGroupConcat(
            SeaTunnelRowType sourceType, SeaTunnelTransform transform, DataStream<Row> stream) {
        TypeInformation rowTypeInfo =
                TypeConverterUtils.convert(
                        transform.getProducedCatalogTable().getSeaTunnelRowType());
        FlinkRowConverter transformInputRowConverter = new FlinkRowConverter(sourceType);
        FlinkRowConverter transformOutputRowConverter =
                new FlinkRowConverter(transform.getProducedCatalogTable().getSeaTunnelRowType());

        // 使用窗口处理 GroupConcat
        DataStream<Row> output = stream
                .keyBy(r -> 1) // 使用固定的key，将所有数据分到同一组
                .process(
                        new GroupConcatWindowProcessFunction(transform, transformInputRowConverter, transformOutputRowConverter),
                        rowTypeInfo
                );

        return output;
    }

    private static class GroupConcatWindowProcessFunction extends KeyedProcessFunction<Integer, Row, Row> {
        private final SeaTunnelTransform transform;
        private final FlinkRowConverter inputConverter;
        private final FlinkRowConverter outputConverter;
        private List<Row> bufferedInputs = new ArrayList<>();

        public GroupConcatWindowProcessFunction(
                SeaTunnelTransform transform,
                FlinkRowConverter inputConverter,
                FlinkRowConverter outputConverter) {
            this.transform = transform;
            this.inputConverter = inputConverter;
            this.outputConverter = outputConverter;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            transform.open();
        }

        @Override
        public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
            SeaTunnelRow seaTunnelRow = inputConverter.reconvert(value);

            // 调用 mapList 将数据插入 SQLite
            transform.mapList(seaTunnelRow);
            bufferedInputs.add(value);

            // 注册定时器，在数据流结束时触发
            ctx.timerService().registerEventTimeTimer(Long.MAX_VALUE - 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
            if (timestamp == Long.MAX_VALUE - 1) {
                try (GroupConcatQueryResult queryResult = transform.executeGroupConcatQuery()) {
                    ResultSet resultSet = queryResult.getResultSet();
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    while (resultSet.next()) {
                        Object[] newFields = new Object[columnCount];
                        for (int i = 1; i <= newFields.length; i++) {
                            Object value = resultSet.getObject(i);
                            newFields[i - 1] = value;
                        }
                        SeaTunnelRow newRow = new SeaTunnelRow(newFields);
                        newRow.setRowKind(RowKind.INSERT);
                        Row outputRow = outputConverter.convert(newRow);
                        out.collect(outputRow);
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            transform.close();
        }
    }

    protected DataStream<Row> joinStream(Config pluginConfig) {
        StreamTableEnvironment tableEnv = flinkRuntimeEnvironment.getStreamTableEnvironment();
        Table joinTable = tableEnv.sqlQuery(pluginConfig.getString("query"));
        TypeInformation<Row> typeInfo = joinTable.getSchema().toRowType();
        DataStream<Tuple2<Boolean, Row>> retractStream =
                tableEnv.toRetractStream(joinTable, typeInfo);

        // BATCH: 在 endInput 物化 retract，按左表业务键保留最后一条，去掉 LEFT JOIN 中间空关联行
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
     * BATCH 专用：消费 retract 流，输入结束时只输出每个左表键的最终行。
     * LEFT JOIN 常见序列为 +I(空右表) -> -D(空右表) -> +I(有关联)，只保留最后一次 +I。
     */
    private static class RetractBatchMaterializeOperator
            extends AbstractStreamOperator<Row>
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

        /** 左表业务键：姓名 + 论文名称（对应 SELECT 前两段左表字段中的第 0、2 列） */
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
}
