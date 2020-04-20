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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class MessagePublishBufferThrottleTest extends BrokerTestBase {

    @Override
    protected void setup() throws Exception {
        //No-op
    }

    @Override
    protected void cleanup() throws Exception {
        //No-op
    }

    @Test
    public void testMessagePublishBufferThrottleDisabled() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(-1);
        conf.setMessagePublishBufferCheckIntervalInMillis(10);
        super.baseSetup();
        final String topic = "persistent://prop/ns-abc/testMessagePublishBufferThrottleDisabled";
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .producerName("producer-name")
            .create();
        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        ((AbstractTopic)topicRef).getProducer("producer-name").getCnx().setMessagePublishBufferSize(Long.MAX_VALUE / 2);
        Thread.sleep(20);
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        // Make sure the producer can publish succeed.
        for (int i = 0; i < 10; i++) {
            futures.add(producer.sendAsync(new byte[1024 * 1024]));
        }
        FutureUtil.waitForAll(futures).get();
        for (CompletableFuture<MessageId> future : futures) {
            Assert.assertNotNull(future.get());
        }
        Thread.sleep(20);
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        super.internalCleanup();
    }

    @Test
    public void testMessagePublishBufferThrottleEnable() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(1);
        conf.setMessagePublishBufferCheckIntervalInMillis(2);
        super.baseSetup();
        Thread.sleep(4);
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        final String topic = "persistent://prop/ns-abc/testMessagePublishBufferThrottleEnable";
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .producerName("producer-name")
            .create();
        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        ((AbstractTopic)topicRef).getProducer("producer-name").getCnx().setMessagePublishBufferSize(Long.MAX_VALUE / 2);
        Thread.sleep(4);
        Assert.assertTrue(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        // The first message can publish success, but the second message should be blocked
        producer.sendAsync(new byte[1024]).get(1, TimeUnit.SECONDS);
        MessageId messageId = null;
        try {
            messageId = producer.sendAsync(new byte[1024]).get(1, TimeUnit.SECONDS);
            Assert.fail("should failed, because producer blocked by publish buffer limiting");
        } catch (TimeoutException e) {
            // No-op
        }
        Assert.assertNull(messageId);

        ((AbstractTopic)topicRef).getProducer("producer-name").getCnx().setMessagePublishBufferSize(0L);
        Thread.sleep(4);

        List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        // Make sure the producer can publish succeed.
        for (int i = 0; i < 10; i++) {
            futures.add(producer.sendAsync(new byte[1024 * 1024]));
        }
        FutureUtil.waitForAll(futures).get();
        for (CompletableFuture<MessageId> future : futures) {
            Assert.assertNotNull(future.get());
        }
        Thread.sleep(4);
        Assert.assertEquals(pulsar.getBrokerService().getCurrentMessagePublishBufferSize(), 0L);
        super.internalCleanup();
    }

    @Test
    public void testBlockByPublishRateLimiting() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(1);
        conf.setMessagePublishBufferCheckIntervalInMillis(2);
        super.baseSetup();
        Thread.sleep(4);
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        final String topic = "persistent://prop/ns-abc/testMessagePublishBufferThrottleEnable";
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .producerName("producer-name")
            .create();
        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        ((AbstractTopic)topicRef).getProducer("producer-name").getCnx().setMessagePublishBufferSize(Long.MAX_VALUE / 2);
        producer.sendAsync(new byte[1024]).get(1, TimeUnit.SECONDS);

        Thread.sleep(4);
        ((AbstractTopic)topicRef).getProducer("producer-name").getCnx().setAutoReadDisabledRateLimiting(true);
        ((AbstractTopic)topicRef).getProducer("producer-name").getCnx().setMessagePublishBufferSize(0);
        Thread.sleep(4);
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        MessageId messageId = null;
        try {
            messageId = producer.sendAsync(new byte[1024]).get(1, TimeUnit.SECONDS);
            Assert.fail("should failed, because producer blocked by publish buffer limiting");
        } catch (TimeoutException e) {
            // No-op
        }
        Assert.assertNull(messageId);

        ((AbstractTopic)topicRef).getProducer("producer-name").getCnx().setAutoReadDisabledRateLimiting(false);
        ((AbstractTopic)topicRef).getProducer("producer-name").getCnx().enableCnxAutoRead();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        // Make sure the producer can publish succeed.
        for (int i = 0; i < 10; i++) {
            futures.add(producer.sendAsync(new byte[1024 * 1024]));
        }
        FutureUtil.waitForAll(futures).get();
        for (CompletableFuture<MessageId> future : futures) {
            Assert.assertNotNull(future.get());
        }
        Thread.sleep(4);
        Assert.assertEquals(pulsar.getBrokerService().getCurrentMessagePublishBufferSize(), 0L);
        super.internalCleanup();
    }
}
