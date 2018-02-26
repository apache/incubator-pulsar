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
package org.apache.pulsar.client.impl;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;

@SuppressWarnings("deprecation")
public class PulsarClientImpl implements PulsarClient {

    private static final Logger log = LoggerFactory.getLogger(PulsarClientImpl.class);

    private final ClientConfigurationData conf;
    private final LookupService lookup;
    private final ConnectionPool cnxPool;
    private final Timer timer;
    private final ExecutorProvider externalExecutorProvider;

    enum State {
        Open, Closing, Closed
    }

    private AtomicReference<State> state = new AtomicReference<>();
    private final IdentityHashMap<ProducerBase, Boolean> producers;
    private final IdentityHashMap<ConsumerBase, Boolean> consumers;

    private final AtomicLong producerIdGenerator = new AtomicLong();
    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong requestIdGenerator = new AtomicLong();

    private final EventLoopGroup eventLoopGroup;

    @Deprecated
    public PulsarClientImpl(String serviceUrl, ClientConfiguration conf) throws PulsarClientException {
        this(conf.setServiceUrl(serviceUrl).getConfigurationData().clone());
    }

    @Deprecated
    public PulsarClientImpl(String serviceUrl, ClientConfiguration conf, EventLoopGroup eventLoopGroup)
            throws PulsarClientException {
        this(conf.setServiceUrl(serviceUrl).getConfigurationData().clone(), eventLoopGroup);
    }

    @Deprecated
    public PulsarClientImpl(String serviceUrl, ClientConfiguration conf, EventLoopGroup eventLoopGroup,
            ConnectionPool cnxPool) throws PulsarClientException {
        this(conf.setServiceUrl(serviceUrl).getConfigurationData().clone(), eventLoopGroup, cnxPool);
    }

    public PulsarClientImpl(ClientConfigurationData conf) throws PulsarClientException {
        this(conf, getEventLoopGroup(conf));
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, new ConnectionPool(conf, eventLoopGroup));
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool)
            throws PulsarClientException {
        if (conf == null || isBlank(conf.getServiceUrl()) || eventLoopGroup == null) {
            throw new PulsarClientException.InvalidConfigurationException("Invalid client configuration");
        }
        this.eventLoopGroup = eventLoopGroup;
        this.conf = conf;
        conf.getAuthentication().start();
        this.cnxPool = cnxPool;
        if (conf.getServiceUrl().startsWith("http")) {
            lookup = new HttpLookupService(conf, eventLoopGroup);
        } else {
            lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.isUseTls());
        }
        timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-timer"), 1, TimeUnit.MILLISECONDS);
        externalExecutorProvider = new ExecutorProvider(conf.getNumListenerThreads(), "pulsar-external-listener");
        producers = Maps.newIdentityHashMap();
        consumers = Maps.newIdentityHashMap();
        state.set(State.Open);
    }

    public ClientConfigurationData getConfiguration() {
        return conf;
    }

    @Override
    public ProducerBuilder newProducer() {
        return new ProducerBuilderImpl(this);
    }

    @Override
    public ConsumerBuilder newConsumer() {
        return new ConsumerBuilderImpl(this);
    }

    @Override
    public ReaderBuilder newReader() {
        return new ReaderBuilderImpl(this);
    }

    @Override
    public Producer createProducer(String topic) throws PulsarClientException {
        try {
            ProducerConfigurationData conf = new ProducerConfigurationData();
            conf.setTopicName(topic);
            return createProducerAsync(conf).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public Producer createProducer(final String topic, final ProducerConfiguration conf) throws PulsarClientException {
        if (conf == null) {
            throw new PulsarClientException.InvalidConfigurationException("Invalid null configuration object");
        }

        try {
            ProducerConfigurationData confData = conf.getProducerConfigurationData().clone();
            confData.setTopicName(topic);
            return createProducerAsync(confData).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Producer> createProducerAsync(String topic) {
        ProducerConfigurationData conf = new ProducerConfigurationData();
        conf.setTopicName(topic);
        return createProducerAsync(conf);
    }

    @Override
    public CompletableFuture<Producer> createProducerAsync(final String topic, final ProducerConfiguration conf) {
        ProducerConfigurationData confData = conf.getProducerConfigurationData().clone();
        confData.setTopicName(topic);
        return createProducerAsync(confData);
    }

    public CompletableFuture<Producer> createProducerAsync(ProducerConfigurationData conf) {
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Producer configuration undefined"));
        }

        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        String topic = conf.getTopicName();

        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name"));
        }

        CompletableFuture<Producer> producerCreatedFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ProducerBase producer;
            if (metadata.partitions > 1) {
                producer = new PartitionedProducerImpl(PulsarClientImpl.this, topic, conf, metadata.partitions,
                        producerCreatedFuture);
            } else {
                producer = new ProducerImpl(PulsarClientImpl.this, topic, conf, producerCreatedFuture, -1);
            }

            synchronized (producers) {
                producers.put(producer, Boolean.TRUE);
            }
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata: {}", topic, ex.getMessage());
            producerCreatedFuture.completeExceptionally(ex);
            return null;
        });

        return producerCreatedFuture;
    }

    @Override
    public Consumer subscribe(final String topic, final String subscription) throws PulsarClientException {
        return subscribe(topic, subscription, new ConsumerConfiguration());
    }

    @Override
    public Consumer subscribe(String topic, String subscription, ConsumerConfiguration conf)
            throws PulsarClientException {
        try {
            return subscribeAsync(topic, subscription, conf).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Consumer> subscribeAsync(String topic, String subscription) {
        ConsumerConfigurationData conf = new ConsumerConfigurationData();
        conf.getTopicNames().add(topic);
        conf.setSubscriptionName(subscription);
        return subscribeAsync(conf);
    }

    @Override
    public CompletableFuture<Consumer> subscribeAsync(final String topic, final String subscription,
            final ConsumerConfiguration conf) {
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Invalid null configuration"));
        }

        ConsumerConfigurationData confData = conf.getConfigurationData().clone();
        confData.getTopicNames().add(topic);
        confData.setSubscriptionName(subscription);
        return subscribeAsync(confData);
    }

    public CompletableFuture<Consumer> subscribeAsync(ConsumerConfigurationData conf) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        if (!conf.getTopicNames().stream().allMatch(topic -> TopicName.isValid(topic))) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name"));
        }

        if (isBlank(conf.getSubscriptionName())) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Empty subscription name"));
        }

        if (conf.isReadCompacted() && (!conf.getTopicNames().stream()
                .allMatch(topic -> TopicName.get(topic).getDomain() == TopicDomain.persistent)
                || (conf.getSubscriptionType() != SubscriptionType.Exclusive
                        && conf.getSubscriptionType() != SubscriptionType.Failover))) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Read compacted can only be used with exclusive of failover persistent subscriptions"));
        }

        if (conf.getConsumerEventListener() != null && conf.getSubscriptionType() != SubscriptionType.Failover) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Active consumer listener is only supported for failover subscription"));
        }

        if (conf.getTopicNames().size() == 1) {
            return singleTopicSubscribeAsysnc(conf);
        } else {
            return multiTopicSubscribeAsync(conf);
        }
    }

    private CompletableFuture<Consumer> singleTopicSubscribeAsysnc(ConsumerConfigurationData conf) {
        CompletableFuture<Consumer> consumerSubscribedFuture = new CompletableFuture<>();

        String topic = conf.getSingleTopic();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ConsumerBase consumer;
            // gets the next single threaded executor from the list of executors
            ExecutorService listenerThread = externalExecutorProvider.getExecutor();
            if (metadata.partitions > 1) {
                consumer = new PartitionedConsumerImpl(PulsarClientImpl.this, conf, metadata.partitions, listenerThread,
                        consumerSubscribedFuture);
            } else {
                consumer = new ConsumerImpl(PulsarClientImpl.this, topic, conf, listenerThread, -1,
                        consumerSubscribedFuture);
            }

            synchronized (consumers) {
                consumers.put(consumer, Boolean.TRUE);
            }
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata", topic, ex);
            consumerSubscribedFuture.completeExceptionally(ex);
            return null;
        });

        return consumerSubscribedFuture;
    }

    private CompletableFuture<Consumer> multiTopicSubscribeAsync(ConsumerConfigurationData conf) {
        CompletableFuture<Consumer> consumerSubscribedFuture = new CompletableFuture<>();

        ConsumerBase consumer = new TopicsConsumerImpl(PulsarClientImpl.this, conf,
                externalExecutorProvider.getExecutor(), consumerSubscribedFuture);

        synchronized (consumers) {
            consumers.put(consumer, Boolean.TRUE);
        }

        return consumerSubscribedFuture;
    }

    @Override
    public Reader createReader(String topic, MessageId startMessageId, ReaderConfiguration conf)
            throws PulsarClientException {
        try {
            return createReaderAsync(topic, startMessageId, conf).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Reader> createReaderAsync(String topic, MessageId startMessageId,
            ReaderConfiguration conf) {
        ReaderConfigurationData confData = conf.getReaderConfigurationData().clone();
        confData.setTopicName(topic);
        confData.setStartMessageId(startMessageId);
        return createReaderAsync(confData);
    }

    public CompletableFuture<Reader> createReaderAsync(ReaderConfigurationData conf) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        String topic = conf.getTopicName();

        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidTopicNameException("Invalid topic name"));
        }

        if (conf.getStartMessageId() == null) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Invalid startMessageId"));
        }

        CompletableFuture<Reader> readerFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            if (metadata.partitions > 1) {
                readerFuture.completeExceptionally(
                        new PulsarClientException("Topic reader cannot be created on a partitioned topic"));
                return;
            }

            CompletableFuture<Consumer> consumerSubscribedFuture = new CompletableFuture<>();
            // gets the next single threaded executor from the list of executors
            ExecutorService listenerThread = externalExecutorProvider.getExecutor();
            ReaderImpl reader = new ReaderImpl(PulsarClientImpl.this, conf, listenerThread, consumerSubscribedFuture);

            synchronized (consumers) {
                consumers.put(reader.getConsumer(), Boolean.TRUE);
            }

            consumerSubscribedFuture.thenRun(() -> {
                readerFuture.complete(reader);
            }).exceptionally(ex -> {
                log.warn("[{}] Failed to get create topic reader", topic, ex);
                readerFuture.completeExceptionally(ex);
                return null;
            });
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata", topic, ex);
            readerFuture.completeExceptionally(ex);
            return null;
        });

        return readerFuture;
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        log.info("Client closing. URL: {}", lookup.getServiceUrl());
        if (!state.compareAndSet(State.Open, State.Closing)) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = Lists.newArrayList();

        synchronized (producers) {
            // Copy to a new list, because the closing will trigger a removal from the map
            // and invalidate the iterator
            List<ProducerBase> producersToClose = Lists.newArrayList(producers.keySet());
            producersToClose.forEach(p -> futures.add(p.closeAsync()));
        }

        synchronized (consumers) {
            List<ConsumerBase> consumersToClose = Lists.newArrayList(consumers.keySet());
            consumersToClose.forEach(c -> futures.add(c.closeAsync()));
        }

        FutureUtil.waitForAll(futures).thenRun(() -> {
            // All producers & consumers are now closed, we can stop the client safely
            try {
                shutdown();
                closeFuture.complete(null);
                state.set(State.Closed);
            } catch (PulsarClientException e) {
                closeFuture.completeExceptionally(e);
            }
        }).exceptionally(exception -> {
            closeFuture.completeExceptionally(exception);
            return null;
        });

        return closeFuture;
    }

    @Override
    public void shutdown() throws PulsarClientException {
        try {
            lookup.close();
            cnxPool.close();
            timer.stop();
            externalExecutorProvider.shutdownNow();
            conf.getAuthentication().close();
        } catch (Throwable t) {
            log.warn("Failed to shutdown Pulsar client", t);
            throw new PulsarClientException(t);
        }
    }

    protected CompletableFuture<ClientCnx> getConnection(final String topic) {
        TopicName topicName = TopicName.get(topic);
        return lookup.getBroker(topicName)
                .thenCompose(pair -> cnxPool.getConnection(pair.getLeft(), pair.getRight()));
    }

    protected Timer timer() {
        return timer;
    }

    ExecutorProvider externalExecutorProvider() {
        return externalExecutorProvider;
    }

    long newProducerId() {
        return producerIdGenerator.getAndIncrement();
    }

    long newConsumerId() {
        return consumerIdGenerator.getAndIncrement();
    }

    public long newRequestId() {
        return requestIdGenerator.getAndIncrement();
    }

    public ConnectionPool getCnxPool() {
        return cnxPool;
    }

    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    public CompletableFuture<Integer> getNumberOfPartitions(String topic) {
        return getPartitionedTopicMetadata(topic).thenApply(metadata -> metadata.partitions);
    }

    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(String topic) {

        CompletableFuture<PartitionedTopicMetadata> metadataFuture;

        try {
            TopicName topicName = TopicName.get(topic);
            metadataFuture = lookup.getPartitionedTopicMetadata(topicName);
        } catch (IllegalArgumentException e) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(e.getMessage()));
        }
        return metadataFuture;
    }

    private static EventLoopGroup getEventLoopGroup(ClientConfigurationData conf) {
        ThreadFactory threadFactory = new DefaultThreadFactory("pulsar-client-io");
        return EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), threadFactory);
    }

    void cleanupProducer(ProducerBase producer) {
        synchronized (producers) {
            producers.remove(producer);
        }
    }

    void cleanupConsumer(ConsumerBase consumer) {
        synchronized (consumers) {
            consumers.remove(consumer);
        }
    }
}
