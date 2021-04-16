/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.kafka.connect;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig.PULSAR_SERVICE_URL_CONFIG;

@Slf4j
public class KafkaConnectSink implements Sink<GenericObject> {

    private boolean unwrapKeyValueIfAvailable;

    private final static ImmutableMap<Class<?>, Schema> primitiveTypeToSchema;
    private final static ImmutableMap<SchemaType, Schema> pulsarSchemaTypeTypeToKafkaSchema;

    static {
        primitiveTypeToSchema = ImmutableMap.<Class<?>, Schema>builder()
                .put(Boolean.class, Schema.BOOLEAN_SCHEMA)
                .put(Byte.class, Schema.INT8_SCHEMA)
                .put(Short.class, Schema.INT16_SCHEMA)
                .put(Integer.class, Schema.INT32_SCHEMA)
                .put(Long.class, Schema.INT64_SCHEMA)
                .put(Float.class, Schema.FLOAT32_SCHEMA)
                .put(Double.class, Schema.FLOAT64_SCHEMA)
                .put(String.class, Schema.STRING_SCHEMA)
                .put(byte[].class, Schema.BYTES_SCHEMA)
                .build();
        pulsarSchemaTypeTypeToKafkaSchema = ImmutableMap.<SchemaType, Schema>builder()
                .put(SchemaType.BOOLEAN, Schema.BOOLEAN_SCHEMA)
                .put(SchemaType.INT8, Schema.INT8_SCHEMA)
                .put(SchemaType.INT16, Schema.INT16_SCHEMA)
                .put(SchemaType.INT32, Schema.INT32_SCHEMA)
                .put(SchemaType.INT64, Schema.INT64_SCHEMA)
                .put(SchemaType.FLOAT, Schema.FLOAT32_SCHEMA)
                .put(SchemaType.DOUBLE, Schema.FLOAT64_SCHEMA)
                .put(SchemaType.STRING, Schema.STRING_SCHEMA)
                .put(SchemaType.BYTES, Schema.BYTES_SCHEMA)
                .build();
    }

    private PulsarKafkaSinkContext sinkContext;
    private PulsarKafkaSinkTaskContext taskContext;
    private SinkConnector connector;
    private SinkTask task;

    private int batchSize;
    private long lingerMs;
    private final ScheduledExecutorService scheduledExecutor =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("pulsar-io-kafka-adaptor-sink-flush-%d")
                    .build());
    private final ConcurrentLinkedDeque<Record<GenericObject>> pendingFlushQueue = new ConcurrentLinkedDeque<>();
    private volatile boolean isRunning = false;

    private Properties props = new Properties();
    private PulsarKafkaConnectSinkConfig kafkaSinkConfig;

    protected String topicName;

    @Override
    public void write(Record<GenericObject> sourceRecord) {
        if (log.isDebugEnabled()) {
            log.debug("Record sending to kafka, record={}.", sourceRecord);
        }

        if (!isRunning) {
            log.error("Sink is stopped. Cannot send the record {}", sourceRecord);
            sourceRecord.fail();
            return;
        }

        try {
            SinkRecord record = toSinkRecord(sourceRecord);
            task.put(Lists.newArrayList(record));
        } catch (Exception ex) {
            log.error("Error sending the record {}", sourceRecord, ex);
            sourceRecord.fail();
            return;
        }
        pendingFlushQueue.add(sourceRecord);
        flushIfNeeded(false);
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        flushIfNeeded(true);
        scheduledExecutor.shutdown();
        if (!scheduledExecutor.awaitTermination(10 * lingerMs, TimeUnit.MILLISECONDS)) {
            log.error("scheduledExecutor did not terminate in {} ms", 10 * lingerMs);
        }

        task.stop();
        connector.stop();
        taskContext.close();

        log.info("Kafka sink stopped.");
    }

    @Override
    public void open(Map<String, Object> config, SinkContext ctx) throws Exception {
        kafkaSinkConfig = PulsarKafkaConnectSinkConfig.load(config);
        Objects.requireNonNull(kafkaSinkConfig.getTopic(), "Kafka topic is not set");
        topicName = kafkaSinkConfig.getTopic();
        unwrapKeyValueIfAvailable = kafkaSinkConfig.isUnwrapKeyValueIfAvailable();

        String kafkaConnectorFQClassName = kafkaSinkConfig.getKafkaConnectorSinkClass();
        kafkaSinkConfig.getKafkaConnectorConfigProperties().entrySet()
                .forEach(kv -> props.put(kv.getKey(), kv.getValue()));

        Class<?> clazz = Class.forName(kafkaConnectorFQClassName);
        connector = (SinkConnector) clazz.getConstructor().newInstance();

        Class<? extends Task> taskClass = connector.taskClass();
        sinkContext = new PulsarKafkaSinkContext();
        connector.initialize(sinkContext);
        connector.start(Maps.fromProperties(props));

        List<Map<String, String>> configs = connector.taskConfigs(1);
        configs.forEach(x -> {
            x.put(OFFSET_STORAGE_TOPIC_CONFIG, kafkaSinkConfig.getOffsetStorageTopic());
            x.put(PULSAR_SERVICE_URL_CONFIG, kafkaSinkConfig.getPulsarServiceUrl());
        });
        task = (SinkTask) taskClass.getConstructor().newInstance();
        taskContext =
                new PulsarKafkaSinkTaskContext(configs.get(0), ctx, task::open);
        task.initialize(taskContext);
        task.start(configs.get(0));

        batchSize = kafkaSinkConfig.getBatchSize();
        lingerMs = kafkaSinkConfig.getLingerTimeMs();
        scheduledExecutor.scheduleAtFixedRate(() ->
                this.flushIfNeeded(true), lingerMs, lingerMs, TimeUnit.MILLISECONDS);


        isRunning = true;
        log.info("Kafka sink started : {}.", props);
    }

    private void flushIfNeeded(boolean force) {
        if (force || pendingFlushQueue.stream().limit(batchSize).count() >= batchSize) {
            scheduledExecutor.submit(this::flush);
        }
    }

    // flush always happens on the same thread
    public void flush() {
        if (log.isDebugEnabled()) {
            log.debug("flush requested, pending: {}, batchSize: {}",
                    pendingFlushQueue.size(), batchSize);
        }

        if (pendingFlushQueue.isEmpty()) {
            return;
        }

        final Record<GenericObject> lastNotFlushed = pendingFlushQueue.getLast();
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = taskContext.currentOffsets();

        try {
            task.flush(currentOffsets);
            taskContext.flushOffsets(currentOffsets);
            ackUntil(lastNotFlushed, Record::ack);
        } catch (Throwable t) {
            log.error("error flushing pending records", t);
            ackUntil(lastNotFlushed, Record::fail);
        }
    }

    private void ackUntil(Record<GenericObject> lastNotFlushed, java.util.function.Consumer<Record<GenericObject>> cb) {
        while (!pendingFlushQueue.isEmpty()) {
            Record<GenericObject> r = pendingFlushQueue.pollFirst();
            cb.accept(r);
            if (r == lastNotFlushed) {
                break;
            }
        }
    }

    /**
     * org.apache.kafka.connect.data.Schema for the object
     * @param obj
     * @return org.apache.kafka.connect.data.Schema
     */
    private static Schema getKafkaConnectSchemaForObject(Object obj) {
        if (obj != null && primitiveTypeToSchema.containsKey(obj.getClass())) {
            return primitiveTypeToSchema.get(obj.getClass());
        }
        return null;
    }

    public static Schema getKafkaConnectSchema(org.apache.pulsar.client.api.Schema pulsarSchema, Object obj) {
        if (pulsarSchema != null
                && pulsarSchemaTypeTypeToKafkaSchema.containsKey(pulsarSchema.getSchemaInfo().getType())) {
            return pulsarSchemaTypeTypeToKafkaSchema.get(pulsarSchema.getSchemaInfo().getType());
        }

        Schema result = getKafkaConnectSchemaForObject(obj);
        if (result == null) {
            throw new IllegalStateException("Unsupported kafka schema for Pulsar Schema "
                    + (pulsarSchema == null ? "null" : pulsarSchema.getSchemaInfo().toString())
                    + " object class "
                    + (obj == null ? "null" : obj.getClass().getCanonicalName()));
        }
        return result;
    }


    @SuppressWarnings("rawtypes")
    private SinkRecord toSinkRecord(Record<GenericObject> sourceRecord) {
        final int partition = sourceRecord.getPartitionIndex().orElse(0);
        final String topic = sourceRecord.getTopicName().orElse(topicName);
        final Object key;
        final Object value;
        final Schema keySchema;
        final Schema valueSchema;

        // sourceRecord is never instanceof KVRecord
        // https://github.com/apache/pulsar/pull/10113
        if (unwrapKeyValueIfAvailable && sourceRecord.getSchema() != null
                && sourceRecord.getSchema().getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            KeyValueSchema kvSchema = (KeyValueSchema) sourceRecord.getSchema();
            KeyValue kv = (KeyValue) sourceRecord.getValue().getNativeObject();
            key = kv.getKey();
            value = kv.getValue();
            keySchema = getKafkaConnectSchema(kvSchema.getKeySchema(), key);
            valueSchema = getKafkaConnectSchema(kvSchema.getValueSchema(), value);
        } else {
            key = sourceRecord.getKey().orElse(null);
            value = sourceRecord.getValue().getNativeObject();
            keySchema = Schema.STRING_SCHEMA;
            valueSchema = getKafkaConnectSchema(sourceRecord.getSchema(), value);
        }

        long offset = sourceRecord.getRecordSequence()
                .orElse(-1L);
        if (offset < 0) {
            offset = taskContext.currentOffset(topic, partition)
                    .incrementAndGet();
        } else {
            final long curr = offset;
            taskContext.currentOffset(topic, partition)
                    .updateAndGet(curMax -> Math.max(curr, curMax));
        }

        Long timestamp = null;
        TimestampType timestampType = TimestampType.NO_TIMESTAMP_TYPE;
        if (sourceRecord.getEventTime().isPresent()) {
            timestamp = sourceRecord.getEventTime().get();
            timestampType = TimestampType.CREATE_TIME;
        } else if (sourceRecord.getMessage().isPresent()) {
            timestamp = sourceRecord.getMessage().get().getPublishTime();
            timestampType = TimestampType.LOG_APPEND_TIME;
        }
        SinkRecord sinkRecord = new SinkRecord(topic,
                partition,
                keySchema,
                key,
                valueSchema,
                value,
                offset,
                timestamp,
                timestampType);
        return sinkRecord;
    }

    @VisibleForTesting
    protected long currentOffset(String topic, int partition) {
        return taskContext.currentOffset(topic, partition).get();
    }

}
