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
package org.apache.pulsar.tests.integration.topologies;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.util.FutureUtil;

public class PulsarTestBase {

    public static String randomName(int numChars) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    protected static String generateNamespaceName() {
        return "ns-" + randomName(8);
    }

    protected static String generateTopicName(String topicPrefix, boolean isPersistent) {
        return generateTopicName("default", topicPrefix, isPersistent);
    }

    protected static String generateTopicName(String namespace, String topicPrefix, boolean isPersistent) {
        String topicName = new StringBuilder(topicPrefix)
                .append("-")
                .append(randomName(8))
                .append("-")
                .append(System.currentTimeMillis())
                .toString();
        if (isPersistent) {
            return "persistent://public/" + namespace + "/" + topicName;
        } else {
            return "non-persistent://public/" + namespace + "/" + topicName;
        }
    }

    public void testPublishAndConsume(String serviceUrl, boolean isPersistent) throws Exception {
        String topicName = generateTopicName("testpubconsume", isPersistent);

        int numMessages = 10;

        try (PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build()) {

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscribe()) {

                try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topicName)
                    .create()) {

                    for (int i = 0; i < numMessages; i++) {
                        producer.send("smoke-message-" + i);
                    }
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<String> m = consumer.receive();
                    assertEquals("smoke-message-" + i, m.getValue());
                }
            }
        }
    }

    public void testBatchMessagePublishAndConsume(String serviceUrl, boolean isPersistent) throws Exception {
        String topicName = generateTopicName("test-batch-publish-consume", isPersistent);

        final int numMessages = 10000;
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build()) {
            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(topicName)
                    .subscriptionName("my-sub")
                    .subscribe()) {

                try (Producer<String> producer = client.newProducer(Schema.STRING)
                        .topic(topicName)
                        .create()) {

                    List<CompletableFuture<MessageId>> futures = new ArrayList<>();
                    for (int i = 0; i < numMessages; i++) {
                        futures.add(producer.sendAsync("smoke-message-" + i));
                    }
                    // Wait for all messages are publish succeed.
                    FutureUtil.waitForAll(futures).get();
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<String> m = consumer.receive();
                    assertEquals("smoke-message-" + i, m.getValue());
                }
            }
        }
    }

}
