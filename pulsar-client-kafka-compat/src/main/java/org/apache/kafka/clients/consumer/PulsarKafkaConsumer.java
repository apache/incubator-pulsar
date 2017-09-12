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
package org.apache.kafka.clients.consumer;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.kafka.compat.MessageIdUtils;
import org.apache.pulsar.client.kafka.compat.PulsarKafkaConfig;
import org.apache.pulsar.client.util.ConsumerName;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import com.google.common.collect.Lists;

public class PulsarKafkaConsumer<K, V> implements Consumer<K, V>, MessageListener {

    private static final long serialVersionUID = 1L;

    private final PulsarClient client;

    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private final String groupId;
    private final boolean isAutoCommit;

    private final ConcurrentMap<String, org.apache.pulsar.client.api.Consumer> consumers = new ConcurrentHashMap<>();

    private final ConcurrentMap<org.apache.pulsar.client.api.Consumer, MessageId> lastReceivedMessageId = new ConcurrentHashMap<>();

    private final Map<TopicPartition, Long> lastReceivedOffset = new ConcurrentHashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> lastCommittedOffset = new ConcurrentHashMap<>();

    private static class QueueItem {
        final org.apache.pulsar.client.api.Consumer consumer;
        final Message message;

        QueueItem(org.apache.pulsar.client.api.Consumer consumer, Message message) {
            this.consumer = consumer;
            this.message = message;
        }
    }

    // Since a single Kafka consumer can receive from multiple topics, we need to multiplex all the different
    // topics/partitions into a single queues
    private final BlockingQueue<QueueItem> receivedMessages = new ArrayBlockingQueue<>(1000);

    public PulsarKafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    public PulsarKafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    public PulsarKafkaConsumer(Properties properties) {
        this(properties, null, null);
    }

    public PulsarKafkaConsumer(Properties properties, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    private PulsarKafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {

        if (keyDeserializer == null) {
            this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    Deserializer.class);
            this.keyDeserializer.configure(config.originals(), true);
        } else {
            this.keyDeserializer = keyDeserializer;
            config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        }

        if (valueDeserializer == null) {
            this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    Deserializer.class);
            this.valueDeserializer.configure(config.originals(), true);
        } else {
            this.valueDeserializer = valueDeserializer;
            config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }

        groupId = config.getString(ConsumerConfig.GROUP_ID_CONFIG);
        isAutoCommit = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);

        String serviceUrl = config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).get(0);

        Properties properties = new Properties();
        config.originals().forEach((k, v) -> properties.put(k, v));
        ClientConfiguration clientConf = PulsarKafkaConfig.getClientConfiguration(properties);
        try {
            client = PulsarClient.create(serviceUrl, clientConf);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void received(org.apache.pulsar.client.api.Consumer consumer, Message msg) {
        // Block listener thread if the application is slowing down
        try {
            receivedMessages.put(new QueueItem(consumer, msg));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<TopicPartition> assignment() {
        throw new UnsupportedOperationException("Cannot access the partitions assignements");
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
     *
     * @return The set of topics currently subscribed to
     */
    @Override
    public Set<String> subscription() {
        return consumers.keySet();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        topics.forEach(topic -> {
            try {
                // Create individual subscription on each partition, that way we can keep using the
                // acknowledgeCumulative()
                PartitionedTopicMetadata partitionMetadata = ((PulsarClientImpl) client)
                        .getPartitionedTopicMetadata(topic).get();

                ConsumerConfiguration conf = new ConsumerConfiguration();
                conf.setSubscriptionType(SubscriptionType.Failover);
                conf.setMessageListener(this);
                if (partitionMetadata.partitions > 1) {
                    // Subscribe to each partition
                    conf.setConsumerName(ConsumerName.generateRandomName());
                    for (int i = 0; i < partitionMetadata.partitions; i++) {
                        String partitionName = DestinationName.get(topic).getPartition(i).toString();
                        org.apache.pulsar.client.api.Consumer consumer = client.subscribe(partitionName, groupId, conf);
                        consumers.put(partitionName, consumer);
                    }
                } else {
                    // Topic has a single partition
                    org.apache.pulsar.client.api.Consumer consumer = client.subscribe(topic, groupId, conf);
                    consumers.put(topic, consumer);
                }
            } catch (InterruptedException | ExecutionException | PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        throw new UnsupportedOperationException("ConsumerRebalanceListener is not supported");
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException("Cannot manually assign partitions");
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        throw new UnsupportedOperationException("Cannot subscribe with topic name pattern");
    }

    @Override
    public void unsubscribe() {
        consumers.values().forEach(c -> {
            try {
                c.unsubscribe();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public ConsumerRecords<K, V> poll(long timeoutMillis) {
        try {
            QueueItem item = receivedMessages.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            if (item == null) {
                return (ConsumerRecords<K, V>) ConsumerRecords.EMPTY;
            }

            if (isAutoCommit) {
                // Commit the offset of previously dequeued messages
                commitAsync();
            }

            String topic = item.consumer.getTopic();
            Message msg = item.message;
            MessageIdImpl msgId = (MessageIdImpl) msg.getMessageId();
            long offset = MessageIdUtils.getOffset(msgId);

            int partition = msgId.getPartitionIndex();

            TopicPartition tp = new TopicPartition(topic, partition);

            K key = getKey(topic, msg);
            V value = valueDeserializer.deserialize(topic, msg.getData());

            TimestampType timestampType = TimestampType.LOG_APPEND_TIME;
            long timestamp = msg.getPublishTime();

            if (msg.getEventTime() > 0) {
                // If we have Event time, use that in preference
                timestamp = msg.getEventTime();
                timestampType = TimestampType.CREATE_TIME;
            }

            ConsumerRecord<K, V> consumerRecord = new ConsumerRecord<>(topic, partition, offset, timestamp,
                    timestampType, -1, msg.hasKey() ? msg.getKey().length() : 0, msg.getData().length, key, value);

            Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();
            records.put(tp, Lists.newArrayList(consumerRecord));

            // Update last message id seen by application
            lastReceivedMessageId.put(item.consumer, msgId);
            lastReceivedOffset.put(tp, offset);
            return new ConsumerRecords<>(records);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private K getKey(String topic, Message msg) {
        if (!msg.hasKey()) {
            return null;
        }

        if (keyDeserializer instanceof StringDeserializer) {
            return (K) msg.getKey();
        } else {
            // Assume base64 encoding
            byte[] data = Base64.getDecoder().decode(msg.getKey());
            return keyDeserializer.deserialize(topic, data);
        }
    }

    @Override
    public void commitSync() {
        try {
            doCommitOffsets(getCurrentOffsetsMap()).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            doCommitOffsets(offsets).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitAsync() {
        doCommitOffsets(getCurrentOffsetsMap());
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        Map<TopicPartition, OffsetAndMetadata> offsets = getCurrentOffsetsMap();
        doCommitOffsets(offsets).handle((v, throwable) -> {
            callback.onComplete(offsets, throwable != null ? new Exception(throwable) : null);
            return null;
        });
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        doCommitOffsets(offsets).handle((v, throwable) -> {
            callback.onComplete(offsets, throwable != null ? new Exception(throwable) : null);
            return null;
        });
    }

    private CompletableFuture<Void> doCommitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        offsets.forEach((topicPartition, offsetAndMetadata) -> {
            String topicName = DestinationName.get(topicPartition.topic()).getPartition(topicPartition.partition())
                    .toString();
            org.apache.pulsar.client.api.Consumer consumer = consumers.get(topicName);

            lastCommittedOffset.put(topicPartition, offsetAndMetadata);
            futures.add(consumer.acknowledgeCumulativeAsync(MessageIdUtils.getMessageId(offsetAndMetadata.offset())));
        });

        return FutureUtil.waitForAll(futures);
    }

    private Map<TopicPartition, OffsetAndMetadata> getCurrentOffsetsMap() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        lastReceivedMessageId.forEach((consumer, messageId) -> {
            DestinationName dn = DestinationName.get(consumer.getTopic());
            int partition = dn.isPartitioned() ? dn.getPartitionIndex() : 0;

            TopicPartition tp = new TopicPartition(dn.getPartitionedTopicName(), partition);
            OffsetAndMetadata om = new OffsetAndMetadata(MessageIdUtils.getOffset(messageId));
            offsets.put(tp, om);
        });

        return offsets;
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position(TopicPartition partition) {
        return lastReceivedOffset.get(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return lastCommittedOffset.get(partition);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<TopicPartition> paused() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        try {
            if (isAutoCommit) {
                commitAsync();
            }
            client.closeAsync().get(timeout, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void wakeup() {
        throw new UnsupportedOperationException();
    }
}
