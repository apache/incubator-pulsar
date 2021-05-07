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
package org.apache.pulsar.functions.source;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.ExtendedSourceContext;
import org.apache.pulsar.io.core.SourceContext;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class SingleConsumerPulsarSource<T> extends PulsarSource<T> {

    private final PulsarClient pulsarClient;
    private final SingleConsumerPulsarSourceConfig pulsarSourceConfig;
    private final Map<String, String> properties;
    private final ClassLoader functionClassLoader;
    private final TopicSchema topicSchema;
    private Consumer<T> consumer;

    public SingleConsumerPulsarSource(PulsarClient pulsarClient,
                                      SingleConsumerPulsarSourceConfig pulsarSourceConfig,
                                      Map<String, String> properties,
                                      ClassLoader functionClassLoader) {
        super(pulsarClient, pulsarSourceConfig, properties, functionClassLoader);
        this.pulsarClient = pulsarClient;
        this.pulsarSourceConfig = pulsarSourceConfig;
        this.topicSchema = new TopicSchema(pulsarClient);
        this.properties = properties;
        this.functionClassLoader = functionClassLoader;
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        log.info("Opening pulsar source with config: {}", pulsarSourceConfig);

        Class<?> typeArg = Reflections.loadClass(this.pulsarSourceConfig.getTypeClassName(),
                this.functionClassLoader);

        checkArgument(!Void.class.equals(typeArg), "Input type of Pulsar Function cannot be Void");

        String topic = pulsarSourceConfig.getTopic();
        PulsarSourceConsumerConfig<T> pulsarSourceConsumerConfig
                = buildPulsarSourceConsumerConfig(topic, pulsarSourceConfig.getConsumerConfig(), typeArg);

        log.info("Creating consumer for topic : {}, schema : {}, schemaInfo: {}", topic, pulsarSourceConsumerConfig.getSchema(), pulsarSourceConsumerConfig.getSchema().getSchemaInfo());

        ConsumerBuilder<T> cb = createConsumeBuilder(topic, pulsarSourceConsumerConfig);
        consumer = cb.subscribeAsync().join();
        if (sourceContext instanceof ExtendedSourceContext) {
            ((ExtendedSourceContext) sourceContext).setConsumerGetter(topicName -> {
                try {
                    TopicName src = TopicName.get(topic);
                    if (src.equals(TopicName.get(topicName))) {
                        return consumer;
                    }
                } catch (Exception e) {
                    return null;
                }
                return null;
            });
        }
    }

    @Override
    public Record<T> read() throws Exception {
        Message<T> message = consumer.receive();
        return buildRecord(consumer, message);
    }

    @VisibleForTesting
    Consumer<T> getInputConsumer() {
        return consumer;
    }

    @Override
    public void close() throws Exception {
        if (consumer != null ) {
            consumer.close();
        }
    }
}
