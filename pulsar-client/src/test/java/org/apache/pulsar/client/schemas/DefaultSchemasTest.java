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
package org.apache.pulsar.client.schemas;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schemas.StringSchema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class DefaultSchemasTest {
    private PulsarClient client;

    @BeforeClass
    public void setup() throws PulsarClientException {
        client = PulsarClient.builder().build();
    }

    @Test
    public void testConsumerInstantiation() {
        ConsumerBuilder<String> stringConsumerBuilder = client.newConsumer(new StringSchema());
        Arrays.asList(stringConsumerBuilder).forEach(Assert::assertNotNull);
    }

    @Test
    public void testProducerInstantiation() {
        ProducerBuilder<String> stringProducerBuilder = client.newProducer(new StringSchema());
        Arrays.asList(stringProducerBuilder).forEach(Assert::assertNotNull);
    }

    @Test
    public void testStringSchema() {
        String testString = "hello world️";
        byte[] bytes = testString.getBytes();
        StringSchema stringSchema = new StringSchema();
        assertEquals(stringSchema.decode(bytes), testString);
        assertEquals(stringSchema.encode(testString), bytes);

        Message<String> msg1 = MessageBuilder.create(stringSchema)
                .setContent(bytes)
                .build();
        Assert.assertEquals(stringSchema.decode(msg1.getData()), testString);

        Message<String> msg2 = MessageBuilder.create(stringSchema)
                .setValue(testString)
                .build();
        Assert.assertEquals(stringSchema.encode(testString), msg2.getData());

        StringSchema stringSchemaUtf16 = new StringSchema(StandardCharsets.UTF_16);
        assertEquals(stringSchemaUtf16.decode(bytes), testString);
        assertEquals(stringSchemaUtf16.encode(testString), bytes);
    }
}
