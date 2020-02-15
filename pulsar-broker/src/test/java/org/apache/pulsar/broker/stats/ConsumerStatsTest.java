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
package org.apache.pulsar.broker.stats;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Slf4j
public class ConsumerStatsTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setMaxUnackedMessagesPerConsumer(0);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConsumerStatsOnZeroMaxUnackedMessagesPerConsumer() throws PulsarClientException, InterruptedException, PulsarAdminException {
        Assert.assertEquals(pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer(), 0);
        final String topicName = "persistent://my-property/my-ns/testConsumerStatsOnZeroMaxUnackedMessagesPerConsumer";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            // don't ack messages here
            consumer.receive();
            received++;
        }

        Assert.assertEquals(received, messages);
        received = 0;

        TopicStats stats = admin.topics().getStats(topicName);
        Assert.assertEquals(stats.subscriptions.size(), 1);
        Assert.assertEquals(stats.subscriptions.entrySet().iterator().next().getValue().consumers.size(), 1);
        Assert.assertFalse(stats.subscriptions.entrySet().iterator().next().getValue().consumers.get(0).blockedConsumerOnUnackedMsgs);
        Assert.assertEquals(stats.subscriptions.entrySet().iterator().next().getValue().consumers.get(0).unackedMessages, messages);

        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }

        Assert.assertEquals(received, messages);

        // wait acknowledge send
        Thread.sleep(2000);

        stats = admin.topics().getStats(topicName);

        Assert.assertFalse(stats.subscriptions.entrySet().iterator().next().getValue().consumers.get(0).blockedConsumerOnUnackedMsgs);
        Assert.assertEquals(stats.subscriptions.entrySet().iterator().next().getValue().consumers.get(0).unackedMessages, 0);
    }

}
