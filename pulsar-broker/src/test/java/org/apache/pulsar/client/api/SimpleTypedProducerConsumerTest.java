/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SimpleTypedProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(SimpleTypedProducerConsumerTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testJsonProducerAndConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        JSONSchema<JsonEncodedPojo> jsonSchema =
            JSONSchema.of(JsonEncodedPojo.class);

        Consumer<JsonEncodedPojo> consumer = pulsarClient
            .newConsumer(jsonSchema)
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscribe();

        Producer<JsonEncodedPojo> producer = pulsarClient
            .newProducer(jsonSchema)
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .create();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(new JsonEncodedPojo(message));
        }

        Message<JsonEncodedPojo> msg = null;
        Set<JsonEncodedPojo> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            JsonEncodedPojo receivedMessage = msg.getValue();
            log.debug("Received message: [{}]", receivedMessage);
            JsonEncodedPojo expectedMessage = new JsonEncodedPojo("my-message-" + i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        SchemaRegistry.SchemaAndMetadata storedSchema = pulsar.getSchemaRegistryService()
            .getSchema("my-property/my-ns/my-topic1")
            .get();

        Assert.assertEquals(storedSchema.schema.getData(), jsonSchema.getSchemaInfo().getSchema());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testJsonProducerAndConsumerWithPrestoredSchema() throws Exception {
        log.info("-- Starting {} test --", methodName);

        JSONSchema<JsonEncodedPojo> jsonSchema =
            JSONSchema.of(JsonEncodedPojo.class);

        pulsar.getSchemaRegistryService()
            .putSchemaIfAbsent("my-property/my-ns/my-topic1",
                SchemaData.builder()
                    .type(SchemaType.JSON)
                    .isDeleted(false)
                    .timestamp(Clock.systemUTC().millis())
                    .user("me")
                    .data(jsonSchema.getSchemaInfo().getSchema())
                    .props(Collections.emptyMap())
                    .build()
            ).get();

        Consumer<JsonEncodedPojo> consumer = pulsarClient
            .newConsumer(jsonSchema)
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscribe();

        Producer<JsonEncodedPojo> producer = pulsarClient
            .newProducer(jsonSchema)
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .create();

        consumer.close();
        producer.close();

        SchemaRegistry.SchemaAndMetadata storedSchema = pulsar.getSchemaRegistryService()
            .getSchema("my-property/my-ns/my-topic1")
            .get();

        Assert.assertEquals(storedSchema.schema.getData(), jsonSchema.getSchemaInfo().getSchema());

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(expectedExceptions = {PulsarClientException.class})
    public void testJsonConsumerWithWrongPrestoredSchema() throws Exception {
        log.info("-- Starting {} test --", methodName);

        byte[] randomSchemaBytes = new byte[20];
        SecureRandom.getInstanceStrong().nextBytes(randomSchemaBytes);

        pulsar.getSchemaRegistryService()
            .putSchemaIfAbsent("my-property/my-ns/my-topic1",
                SchemaData.builder()
                    .type(SchemaType.JSON)
                    .isDeleted(false)
                    .timestamp(Clock.systemUTC().millis())
                    .user("me")
                    .data(randomSchemaBytes)
                    .props(Collections.emptyMap())
                    .build()
            ).get();

        Consumer<JsonEncodedPojo> consumer = pulsarClient
            .newConsumer(JSONSchema.of(JsonEncodedPojo.class))
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscribe();

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(expectedExceptions = {PulsarClientException.class})
    public void testJsonProducerWithWrongPrestoredSchema() throws Exception {
        log.info("-- Starting {} test --", methodName);

        byte[] randomSchemaBytes = new byte[20];
        SecureRandom.getInstanceStrong().nextBytes(randomSchemaBytes);

        pulsar.getSchemaRegistryService()
            .putSchemaIfAbsent("my-property/my-ns/my-topic1",
                SchemaData.builder()
                    .type(SchemaType.JSON)
                    .isDeleted(false)
                    .timestamp(Clock.systemUTC().millis())
                    .user("me")
                    .data(randomSchemaBytes)
                    .props(Collections.emptyMap())
                    .build()
            ).get();

        Producer<JsonEncodedPojo> producer = pulsarClient
            .newProducer(JSONSchema.of(JsonEncodedPojo.class))
            .topic("persistent://my-property/use/my-ns/my-topic1")
            .create();


        log.info("-- Exiting {} test --", methodName);
    }

    public static class JsonEncodedPojo {
        private String message;

        public JsonEncodedPojo() {
        }

        public JsonEncodedPojo(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JsonEncodedPojo that = (JsonEncodedPojo) o;
            return Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("message", message)
                .toString();
        }
    }

}
