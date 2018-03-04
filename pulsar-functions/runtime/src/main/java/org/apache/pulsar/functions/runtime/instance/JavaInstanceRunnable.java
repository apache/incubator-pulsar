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

package org.apache.pulsar.functions.runtime.instance;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.container.InstanceConfig;
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.Reflections;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceRunnable implements AutoCloseable, Runnable {

    // The class loader that used for loading functions
    private ClassLoader fnClassLoader;
    private final InstanceConfig instanceConfig;
    private final FunctionConfig.ProcessingGuarantees processingGuarantees;
    private final FunctionCacheManager fnCache;
    private final LinkedBlockingQueue<InputMessage> queue;
    private final String jarFile;

    // source topic consumer & sink topic produder
    private final PulsarClientImpl client;
    private Producer sinkProducer;
    private Map<String, Consumer> sourceConsumers;
    @Getter
    private Exception failureException;
    private JavaInstance javaInstance;

    private Map<String, SerDe> inputSerDe;
    private SerDe outputSerDe;

    @Getter
    @Setter
    private class InputMessage {
        private Message actualMessage;
        String topicName;
        SerDe inputSerDe;
        Consumer consumer;
    }

    // function stats
    private final FunctionStats stats;

    public JavaInstanceRunnable(InstanceConfig instanceConfig,
                                int maxBufferedTuples,
                                FunctionCacheManager fnCache,
                                String jarFile,
                                PulsarClient pulsarClient) {
        this.instanceConfig = instanceConfig;
        this.processingGuarantees = instanceConfig.getFunctionConfig().getProcessingGuarantees() == null
                ? FunctionConfig.ProcessingGuarantees.ATMOST_ONCE
                : instanceConfig.getFunctionConfig().getProcessingGuarantees();
        this.fnCache = fnCache;
        this.queue = new LinkedBlockingQueue<>(maxBufferedTuples);
        this.jarFile = jarFile;
        this.client = (PulsarClientImpl) pulsarClient;
        this.stats = new FunctionStats();
    }

    /**
     * The core logic that initialize the instance thread and executes the function
     */
    @Override
    public void run() {
        try {
            log.info("Starting Java Instance {}", instanceConfig.getFunctionConfig().getName());

            // start the function thread
            loadJars();

            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();

            // create the serde
            this.inputSerDe = new HashMap<>();
            instanceConfig.getFunctionConfig().getInputsMap().forEach((k, v) -> this.inputSerDe.put(k, initializeSerDe(v, clsLoader)));

            // start the sink producer
            startSinkProducer();
            // start the source consumer
            startSourceConsumers();

            // initialize the thread context
            ThreadContext.put("function", FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()));

            this.outputSerDe = initializeSerDe(instanceConfig.getFunctionConfig().getOutputSerdeClassName(), clsLoader);

            javaInstance = new JavaInstance(instanceConfig, clsLoader, client,
                    new ArrayList(inputSerDe.values()), outputSerDe, sourceConsumers);

            while (true) {
                JavaExecutionResult result;
                InputMessage msg;
                try {
                    msg = queue.take();
                    log.debug("Received message: {}", msg.getActualMessage().getMessageId());
                } catch (InterruptedException ie) {
                    log.info("Function thread {} is interrupted",
                            FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()), ie);
                    break;
                }

                // process the message

                long processAt = System.nanoTime();
                stats.incrementProcessed();
                Object input = msg.getInputSerDe().deserialize(msg.getActualMessage().getData());
                result = javaInstance.handleMessage(
                        msg.getActualMessage().getMessageId(),
                        msg.getTopicName(),
                        input);
                log.debug("Got result: {}", result.getResult());
                processResult(msg, result, processAt);
            }

            javaInstance.close();
        } catch (Exception ex) {
            log.info("Uncaught exception in Java Instance", ex);
            failureException = ex;
        }
    }

    private void loadJars() throws Exception {
        log.info("Loading JAR files for function {}", instanceConfig);
        // create the function class loader
        fnCache.registerFunctionInstance(
            instanceConfig.getFunctionId(),
            instanceConfig.getInstanceId(),
            Arrays.asList(jarFile),
            Collections.emptyList());
        log.info("Initialize function class loader for function {} at function cache manager",
            instanceConfig.getFunctionConfig().getName());

        this.fnClassLoader = fnCache.getClassLoader(instanceConfig.getFunctionId());
        if (null == fnClassLoader) {
            throw new Exception("No function class loader available.");
        }

        // make sure the function class loader is accessible thread-locally
        Thread.currentThread().setContextClassLoader(fnClassLoader);
    }

    private void startSinkProducer() throws Exception {
        if (instanceConfig.getFunctionConfig().getSinkTopic() != null && !instanceConfig.getFunctionConfig().getSinkTopic().isEmpty()) {
            log.info("Starting Producer for Sink Topic " + instanceConfig.getFunctionConfig().getSinkTopic());
            ProducerConfiguration conf = new ProducerConfiguration();
            conf.setBlockIfQueueFull(true);
            conf.setBatchingEnabled(true);
            conf.setBatchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
            conf.setMaxPendingMessages(1000000);

            this.sinkProducer = client.createProducer(instanceConfig.getFunctionConfig().getSinkTopic(), conf);
        }
    }

    private void startSourceConsumers() throws Exception {
        log.info("Consumer map {}", instanceConfig.getFunctionConfig());
        sourceConsumers = new HashMap<>();
        for (Map.Entry<String, String> entry : instanceConfig.getFunctionConfig().getInputsMap().entrySet()) {
            log.info("Starting Consumer for topic " + entry.getKey());
            ConsumerConfiguration conf = new ConsumerConfiguration();
            if (instanceConfig.getFunctionConfig().getSubscriptionType() == null
                    || instanceConfig.getFunctionConfig().getSubscriptionType() == FunctionConfig.SubscriptionType.SHARED) {
                conf.setSubscriptionType(SubscriptionType.Shared);
            } else {
                conf.setSubscriptionType(SubscriptionType.Exclusive);
            }
            SerDe inputSerde = inputSerDe.get(entry.getKey());
            conf.setMessageListener((consumer, msg) -> {
                try {
                    InputMessage message = new InputMessage();
                    message.setConsumer(consumer);
                    message.setInputSerDe(inputSerde);
                    message.setActualMessage(msg);
                    message.setTopicName(entry.getKey());
                    queue.put(message);
                    if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATMOST_ONCE) {
                        if (instanceConfig.getFunctionConfig().getAutoAck()) {
                            consumer.acknowledgeAsync(msg);
                        }
                    }
                } catch (InterruptedException e) {
                    log.error("Function container {} is interrupted on enqueuing messages",
                            Thread.currentThread().getId(), e);
                }
            });

            this.sourceConsumers.put(entry.getKey(), client.subscribe(entry.getKey(),
                    FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()), conf));
        }
    }

    private void processResult(InputMessage msg, JavaExecutionResult result, long processAt) {
         if (result.getUserException() != null) {
            log.info("Encountered user exception when processing message {}", msg, result.getUserException());
            stats.incrementUserExceptions();
        } else if (result.getSystemException() != null) {
            log.info("Encountered system exception when processing message {}", msg, result.getSystemException());
            stats.incrementSystemExceptions();
        } else if (result.getTimeoutException() != null) {
            log.info("Timedout when processing message {}", msg, result.getTimeoutException());
            stats.incrementTimeoutExceptions();
        } else {
            stats.incrementSuccessfullyProcessed();
            if (result.getResult() != null && sinkProducer != null) {
                byte[] output = outputSerDe.serialize(result.getResult());
                if (output != null) {
                    sinkProducer.sendAsync(output)
                            .thenAccept(messageId -> {
                                if (instanceConfig.getFunctionConfig().getAutoAck()) {
                                    if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                                        msg.getConsumer().acknowledgeAsync(msg.getActualMessage());
                                    }
                                }
                            })
                            .exceptionally(cause -> {
                                log.error("Failed to send the process result {} of message {} to sink topic {}",
                                        result, msg, instanceConfig.getFunctionConfig().getSinkTopic(), cause);
                                return null;
                            });
                } else if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                    if (instanceConfig.getFunctionConfig().getAutoAck()) {
                        msg.getConsumer().acknowledgeAsync(msg.getActualMessage());
                    }
                }
            } else if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                if (instanceConfig.getFunctionConfig().getAutoAck()) {
                    msg.getConsumer().acknowledgeAsync(msg.getActualMessage());
                }
            }
        }
    }

    @Override
    public void close() {
        if (sourceConsumers != null) {
            // stop the consumer first, so no more messages are coming in
            sourceConsumers.forEach((k, v) -> {
                try {
                    v.close();
                } catch (PulsarClientException e) {
                    log.warn("Failed to close consumer to source topic {}", k, e);
                }
            });
            sourceConsumers.clear();
        }

        // kill the result producer
        if (null != sinkProducer) {
            try {
                sinkProducer.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close producer to sink topic {}", instanceConfig.getFunctionConfig().getSinkTopic(), e);
            }
            sinkProducer = null;
        }

        // once the thread quits, clean up the instance
        fnCache.unregisterFunctionInstance(
            instanceConfig.getFunctionId(),
            instanceConfig.getInstanceId());
        log.info("Unloading JAR files for function {}", instanceConfig);
    }

    public InstanceCommunication.MetricsData getAndResetMetrics() {
        InstanceCommunication.MetricsData.Builder bldr = InstanceCommunication.MetricsData.newBuilder();
        addSystemMetrics("__total_processed__", stats.getTotalProcessed(), bldr);
        addSystemMetrics("__total_successfully_processed__", stats.getTotalSuccessfullyProcessed(), bldr);
        addSystemMetrics("__total_system_exceptions__", stats.getTotalSystemExceptions(), bldr);
        addSystemMetrics("__total_timeout_exceptions__", stats.getTotalTimeoutExceptions(), bldr);
        addSystemMetrics("__total_user_exceptions__", stats.getTotalUserExceptions(), bldr);
        if (javaInstance != null) {
            InstanceCommunication.MetricsData userMetrics =  javaInstance.getAndResetMetrics();
            if (userMetrics != null) {
                bldr.putAllMetrics(userMetrics.getMetricsMap());
            }
        }
        return bldr.build();
    }

    public InstanceCommunication.FunctionStatus.Builder getFunctionStatus() {
        InstanceCommunication.FunctionStatus.Builder functionStatusBuilder = InstanceCommunication.FunctionStatus.newBuilder();
        functionStatusBuilder.setNumProcessed(stats.getTotalProcessed());
        functionStatusBuilder.setNumSuccessfullyProcessed(stats.getTotalSuccessfullyProcessed());
        functionStatusBuilder.setNumUserExceptions(stats.getTotalUserExceptions());
        functionStatusBuilder.setNumSystemExceptions(stats.getTotalSystemExceptions());
        functionStatusBuilder.setNumTimeouts(stats.getTotalTimeoutExceptions());
        return functionStatusBuilder;
    }

    private static void addSystemMetrics(String metricName, double value, InstanceCommunication.MetricsData.Builder bldr) {
        InstanceCommunication.MetricsData.DataDigest digest =
                InstanceCommunication.MetricsData.DataDigest.newBuilder()
                .setCount(value).setSum(value).setMax(value).setMin(0).build();
        bldr.putMetrics(metricName, digest);
    }

    private static SerDe initializeSerDe(String serdeClassName, ClassLoader clsLoader) {
        if (null == serdeClassName || serdeClassName.isEmpty()) {
            return null;
        } else {
            return Reflections.createInstance(
                    serdeClassName,
                    SerDe.class,
                    clsLoader);
        }
    }
}
