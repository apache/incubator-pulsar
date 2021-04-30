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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.pulsar.io.core.SinkContext;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig.TOPIC_NAMESPACE_CONFIG;

@Slf4j
public class PulsarKafkaSinkTaskContext implements SinkTaskContext {

    private final Map<String, String> config;

    private final OffsetBackingStore offsetStore;
    private final String topicNamespace;
    private final Consumer<Collection<TopicPartition>> onPartitionChange;
    private final AtomicBoolean runRepartition = new AtomicBoolean(false);

    private final ConcurrentHashMap<TopicPartition, AtomicLong> currentOffsets = new ConcurrentHashMap<>();

    public PulsarKafkaSinkTaskContext(Map<String, String> config,
                                      SinkContext ctx,
                                      Consumer<Collection<TopicPartition>> onPartitionChange) {
        this.config = config;

        offsetStore = new PulsarOffsetBackingStore();
        PulsarKafkaWorkerConfig pulsarKafkaWorkerConfig = new PulsarKafkaWorkerConfig(config);
        offsetStore.configure(pulsarKafkaWorkerConfig);
        offsetStore.start();

        this.onPartitionChange = onPartitionChange;
        this.topicNamespace = pulsarKafkaWorkerConfig.getString(TOPIC_NAMESPACE_CONFIG);
    }

    public void close() {
        offsetStore.stop();
    }

    @Override
    public Map<String, String> configs() {
        return config;
    }

    public AtomicLong currentOffset(String topic, int partition) {
        return currentOffset(new TopicPartition(topic, partition));
    }

    public AtomicLong currentOffset(TopicPartition topicPartition) {
        AtomicLong offset = currentOffsets.computeIfAbsent(topicPartition, kv -> {
            List<ByteBuffer> req = Lists.newLinkedList();
            ByteBuffer key = topicPartitionAsKey(topicPartition);
            req.add(key);
            CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
            offsetStore.get(req, (Throwable ex, Map<ByteBuffer, ByteBuffer> result) -> {
                if (ex == null) {
                    if (result != null && result.size() != 0) {
                        Optional<ByteBuffer> val = result.entrySet().stream()
                                .filter(entry -> entry.getKey().equals(key))
                                .findFirst().map(entry -> entry.getValue());
                        if (val.isPresent()) {
                            long received = val.get().getLong();
                            if (log.isDebugEnabled()) {
                                log.debug("read initial offset for {} == {}", topicPartition, received);
                            }
                            offsetFuture.complete(received);
                            return;
                        }
                    }
                    offsetFuture.complete(-1L);
                } else {
                    offsetFuture.completeExceptionally(ex);
                }
            });

            runRepartition.set(true);
            try {
                return new AtomicLong(offsetFuture.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("error getting initial state of {}", topicPartition, e);
                throw new RuntimeException("error getting initial state of " + topicPartition, e);
            } catch (ExecutionException e) {
                log.error("error getting initial state of {}", topicPartition, e);
                throw new RuntimeException("error getting initial state of " + topicPartition, e);            }
        });
        if (runRepartition.compareAndSet(true, false)) {
            onPartitionChange.accept(currentOffsets.keySet());
        }
        return offset;
    }

    public Map<TopicPartition, OffsetAndMetadata> currentOffsets() {
        Map<TopicPartition, OffsetAndMetadata> snapshot = Maps.newHashMapWithExpectedSize(currentOffsets.size());
        currentOffsets.forEach((topicPartition, offset) -> {
            if (offset.get() > 0) {
                snapshot.put(topicPartition,
                        new OffsetAndMetadata(offset.get(), Optional.empty(), null));
            }
        });
        return snapshot;
    }

    private ByteBuffer topicPartitionAsKey(TopicPartition topicPartition) {
        return ByteBuffer.wrap((topicNamespace + "/" + topicPartition.toString()).getBytes(UTF_8));

    }

    private void fillOffsetMap(Map<ByteBuffer, ByteBuffer> offsetMap, TopicPartition topicPartition, long l) {
        ByteBuffer key = topicPartitionAsKey(topicPartition);
        ByteBuffer value = ByteBuffer.allocate(Long.BYTES);
        value.putLong(l);
        value.flip();
        offsetMap.put(key, value);
    }

    @SneakyThrows
    @Override
    public void offset(Map<TopicPartition, Long> map) {
        map.forEach((key, value) -> {
            if (!currentOffsets.containsKey(key)) {
                runRepartition.set(true);
            }
            currentOffsets.put(key, new AtomicLong(value));
        });

        if (runRepartition.compareAndSet(true, false)) {
            onPartitionChange.accept(currentOffsets.keySet());
        }
    }

    @Override
    public void offset(TopicPartition topicPartition, long l) {
        Map<TopicPartition, Long> map = Maps.newHashMap();
        map.put(topicPartition, l);
        this.offset(map);
    }

    @Override
    public void timeout(long l) {
        log.warn("timeout() is called but is not supported currently.");
    }

    @Override
    public Set<TopicPartition> assignment() {
        return currentOffsets.keySet();
    }

    @Override
    public void pause(TopicPartition... topicPartitions) {
        log.warn("pause() is called but is not supported currently.");
    }

    @Override
    public void resume(TopicPartition... topicPartitions) {
        log.warn("resume() is called but is not supported currently.");
    }

    @Override
    public void requestCommit() {
        log.warn("requestCommit() is called but is not supported currently.");
    }

    public void flushOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) throws Exception {
        Map<ByteBuffer, ByteBuffer> offsetMap = Maps.newHashMapWithExpectedSize(offsets.size());

        offsets.forEach((tp, om) -> fillOffsetMap(offsetMap, tp, om.offset()));
        CompletableFuture<Void> result = new CompletableFuture();
        offsetStore.set(offsetMap, (ex, ignore) -> {
            if (ex == null) {
                result.complete(null);
            } else {
                log.error("error flushing offsets for {}", offsets, ex);
                result.completeExceptionally(ex);
            }
        });
        result.get();
    }
}
