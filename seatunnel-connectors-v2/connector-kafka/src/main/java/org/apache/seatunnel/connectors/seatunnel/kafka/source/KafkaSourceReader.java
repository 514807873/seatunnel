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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.shade.com.google.common.collect.Sets;
import org.apache.seatunnel.shade.com.google.common.util.concurrent.RateLimiter;

import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormatErrorHandleWay;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.format.compatible.kafka.connect.json.CompatibleKafkaConnectDeserializationSchema;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.kafka.utils.HttpUtils.sendPostRequest;

@Slf4j
public class KafkaSourceReader implements SourceReader<SeaTunnelRow, KafkaSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;
    private static final long POLL_TIMEOUT = 10000L;

    private final SourceReader.Context context;
    private final ConsumerMetadata metadata;
    private final Set<KafkaSourceSplit> sourceSplits;
    private final Map<Long, Map<TopicPartition, Long>> checkpointOffsetMap;
    private final Map<TopicPartition, KafkaConsumerThread> consumerThreadMap;
    private final ExecutorService executorService;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final MessageFormatErrorHandleWay messageFormatErrorHandleWay;

    private final LinkedBlockingQueue<KafkaSourceSplit> pendingPartitionsQueue;
    private final String st_offset_url =
            System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/recordOffset";
    private final RateLimiter rateLimiter = RateLimiter.create(2.0);
    private Optional<String> flinkJobId = Optional.empty();
    private volatile boolean running = false;
    private Map<String, Long> offsetMap = new HashMap<>();

    KafkaSourceReader(
            ConsumerMetadata metadata,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            Context context,
            MessageFormatErrorHandleWay messageFormatErrorHandleWay) {
        this.metadata = metadata;
        this.context = context;
        this.messageFormatErrorHandleWay = messageFormatErrorHandleWay;
        this.sourceSplits = new HashSet<>();
        this.deserializationSchema = deserializationSchema;
        this.consumerThreadMap = new ConcurrentHashMap<>();
        this.checkpointOffsetMap = new ConcurrentHashMap<>();
        this.executorService =
                Executors.newCachedThreadPool(r -> new Thread(r, "Kafka Source Data Consumer"));
        pendingPartitionsQueue = new LinkedBlockingQueue<>();
        if (context.getEventListener() instanceof DefaultEventProcessor) {
            DefaultEventProcessor defaultEventProcessor =
                    (DefaultEventProcessor) context.getEventListener();
            Class<? extends DefaultEventProcessor> aClass = defaultEventProcessor.getClass();
            try {
                Field field = aClass.getDeclaredField("jobId");
                field.setAccessible(true);
                Object jobid = field.get(defaultEventProcessor);
                this.flinkJobId = Optional.of(String.valueOf(jobid));
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void open() {}

    @Override
    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdownNow();
        }
        offsetMap.forEach(
                (k, v) -> {
                    if (v != 0) {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("flinkJobId", flinkJobId.get());
                        jsonObject.put("fileName", k);
                        jsonObject.put("position", v);
                        try {
                            sendPostRequest(st_offset_url, jsonObject.toJSONString());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
        System.out.println("抽取完成" + offsetMap);
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (!running) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }

        while (pendingPartitionsQueue.size() != 0) {
            sourceSplits.add(pendingPartitionsQueue.poll());
        }
        sourceSplits.forEach(
                sourceSplit ->
                        consumerThreadMap.computeIfAbsent(
                                sourceSplit.getTopicPartition(),
                                s -> {
                                    KafkaConsumerThread thread = new KafkaConsumerThread(metadata);
                                    executorService.submit(thread);
                                    return thread;
                                }));
        sourceSplits.forEach(
                sourceSplit -> {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    try {
                        consumerThreadMap
                                .get(sourceSplit.getTopicPartition())
                                .getTasks()
                                .put(
                                        consumer -> {
                                            try {
                                                Set<TopicPartition> partitions =
                                                        Sets.newHashSet(
                                                                sourceSplit.getTopicPartition());
                                                consumer.assign(partitions);
                                                if (sourceSplit.getStartOffset() >= 0) {
                                                    consumer.seek(
                                                            sourceSplit.getTopicPartition(),
                                                            sourceSplit.getStartOffset());
                                                }
                                                ConsumerRecords<byte[], byte[]> records =
                                                        consumer.poll(
                                                                Duration.ofMillis(POLL_TIMEOUT));
                                                for (TopicPartition partition : partitions) {
                                                    List<ConsumerRecord<byte[], byte[]>>
                                                            recordList = records.records(partition);
                                                    for (ConsumerRecord<byte[], byte[]> record :
                                                            recordList) {
                                                        try {
                                                            if (deserializationSchema
                                                                    instanceof
                                                                    CompatibleKafkaConnectDeserializationSchema) {
                                                                ((CompatibleKafkaConnectDeserializationSchema)
                                                                                deserializationSchema)
                                                                        .deserialize(
                                                                                record, output);
                                                                offsetMap.put(
                                                                        record.partition() + "",
                                                                        record.offset());
                                                            } else {
                                                                deserializationSchema.deserialize(
                                                                        record.value(), output);
                                                                offsetMap.put(
                                                                        record.partition() + "",
                                                                        record.offset());
                                                            }
                                                            // 限流2秒发送一次offset记录请求
                                                            new Thread(
                                                                            () -> {
                                                                                if (rateLimiter
                                                                                        .tryAcquire()) {
                                                                                    JSONObject
                                                                                            param =
                                                                                                    new JSONObject();
                                                                                    param.put(
                                                                                            "flinkJobId",
                                                                                            flinkJobId
                                                                                                    .get());
                                                                                    param.put(
                                                                                            "fileName",
                                                                                            record
                                                                                                    .partition());
                                                                                    param.put(
                                                                                            "position",
                                                                                            record
                                                                                                    .offset());
                                                                                    try {
                                                                                        sendPostRequest(
                                                                                                st_offset_url,
                                                                                                param
                                                                                                        .toString());
                                                                                    } catch (
                                                                                            Exception
                                                                                                    e) {
                                                                                        throw new RuntimeException(
                                                                                                e);
                                                                                    }
                                                                                }
                                                                            })
                                                                    .start();
                                                        } catch (IOException e) {
                                                            if (this.messageFormatErrorHandleWay
                                                                    == MessageFormatErrorHandleWay
                                                                            .SKIP) {
                                                                log.warn(
                                                                        "Deserialize message failed, skip this message, message: {}",
                                                                        new String(record.value()));
                                                                continue;
                                                            }
                                                            throw e;
                                                        }

                                                        if (Boundedness.BOUNDED.equals(
                                                                        context.getBoundedness())
                                                                && record.offset()
                                                                        >= sourceSplit
                                                                                .getEndOffset()) {
                                                            break;
                                                        }
                                                    }
                                                    long lastOffset = -1;
                                                    if (!recordList.isEmpty()) {
                                                        lastOffset =
                                                                recordList
                                                                        .get(recordList.size() - 1)
                                                                        .offset();
                                                        sourceSplit.setStartOffset(lastOffset + 1);
                                                    }

                                                    if (lastOffset >= sourceSplit.getEndOffset()) {
                                                        sourceSplit.setEndOffset(lastOffset);
                                                    }
                                                }
                                            } catch (Exception e) {
                                                completableFuture.completeExceptionally(e);
                                            }
                                            completableFuture.complete(null);
                                        });
                    } catch (InterruptedException e) {
                        throw new KafkaConnectorException(
                                KafkaConnectorErrorCode.CONSUME_DATA_FAILED, e);
                    }
                    completableFuture.join();
                });

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<KafkaSourceSplit> snapshotState(long checkpointId) {
        checkpointOffsetMap.put(
                checkpointId,
                sourceSplits.stream()
                        .collect(
                                Collectors.toMap(
                                        KafkaSourceSplit::getTopicPartition,
                                        KafkaSourceSplit::getStartOffset)));
        return sourceSplits.stream().map(KafkaSourceSplit::copy).collect(Collectors.toList());
    }

    @Override
    public void addSplits(List<KafkaSourceSplit> splits) {
        running = true;
        splits.forEach(
                s -> {
                    try {
                        pendingPartitionsQueue.put(s);
                    } catch (InterruptedException e) {
                        throw new KafkaConnectorException(
                                KafkaConnectorErrorCode.ADD_SPLIT_CHECKPOINT_FAILED, e);
                    }
                });
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this reader will not add new split.");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (!checkpointOffsetMap.containsKey(checkpointId)) {
            log.warn("checkpoint {} do not exist or have already been committed.", checkpointId);
        } else {
            checkpointOffsetMap
                    .remove(checkpointId)
                    .forEach(
                            (topicPartition, offset) -> {
                                try {
                                    consumerThreadMap
                                            .get(topicPartition)
                                            .getTasks()
                                            .put(
                                                    consumer -> {
                                                        if (this.metadata.isCommitOnCheckpoint()) {
                                                            Map<TopicPartition, OffsetAndMetadata>
                                                                    offsets = new HashMap<>();
                                                            if (offset >= 0) {
                                                                offsets.put(
                                                                        topicPartition,
                                                                        new OffsetAndMetadata(
                                                                                offset));
                                                                consumer.commitSync(offsets);
                                                            }
                                                        }
                                                    });
                                } catch (InterruptedException e) {
                                    log.error("commit offset to kafka failed", e);
                                }
                            });
        }
    }
}
