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
package org.apache.pulsar.broker.admin;

import com.google.common.collect.Sets;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

@Slf4j
public class AdminApiMaxUnackedMessages extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("max-unacked-messages", tenantInfo);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        resetConfig();
    }

    @Test
    public void testMaxUnackedMessagesOnConsumers() throws Exception {
        admin.namespaces().createNamespace("max-unacked-messages/default-on-consumers");
        String namespace = "max-unacked-messages/default-on-consumers";
        assertNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace));
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 2*50000);
        assertEquals(2*50000, admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace).intValue());
    }

    @Test
    public void testMaxUnackedMessagesOnSubscription() throws Exception {
        admin.namespaces().createNamespace("max-unacked-messages/default-on-subscription");
        String namespace = "max-unacked-messages/default-on-subscription";
        assertEquals(200000, admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace));
        admin.namespaces().setMaxUnackedMessagesPerSubscription(namespace, 2*200000);
        assertEquals(2*200000, admin.namespaces().getMaxUnackedMessagesPerSubscription(namespace));
    }

    @Test
    public void testMaxUnackedMessagesPerConsumerPriority() throws Exception {
        cleanup();
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setMaxUnackedMessagesPerConsumer(3);
        setup();
        final String namespace = "max-unacked-messages/priority-on-consumers";
        final String topic = "persistent://" + namespace + "/testMaxUnackedMessagesPerConsumerPriority";
        admin.namespaces().createNamespace(namespace);
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        for (int i = 0; i < 50; i++) {
            producer.send("msg".getBytes());
        }
        Awaitility.await().until(()
                -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(topic)));
        assertNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace));
        assertNull(admin.topics().getMaxUnackedMessagesOnConsumer(topic));
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 2);
        admin.topics().setMaxUnackedMessagesOnConsumer(topic, 1);

        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace)));
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getMaxUnackedMessagesOnConsumer(topic)));
        assertEquals(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace).intValue(), 2);
        assertEquals(admin.topics().getMaxUnackedMessagesOnConsumer(topic).intValue(), 1);
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("sub").topic(topic).receiverQueueSize(1).subscribe();
        List<Message> msgs = consumeMsg(consumer, 3);
        assertEquals(msgs.size(), 1);
        //disable topic-level limiter
        admin.topics().setMaxUnackedMessagesOnConsumer(topic, 0);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getMaxUnackedMessagesOnConsumer(topic).intValue(), 3));
        ackMsgs(consumer, msgs);
        //remove topic-level policy, namespace-level should take effect
        admin.topics().removeMaxUnackedMessagesOnConsumer(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin.topics().getMaxUnackedMessagesOnConsumer(topic)));
        msgs = consumeMsg(consumer, 5);
        assertEquals(msgs.size(), 2);
        ackMsgs(consumer, msgs);
        //disable namespace-level limiter
        admin.namespaces().setMaxUnackedMessagesPerConsumer(namespace, 0);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace).intValue(), 0));
        msgs = consumeMsg(consumer, 5);
        assertEquals(msgs.size(), 5);
        ackMsgs(consumer, msgs);
        //remove namespace-level policy, broker-level should take effect
        admin.namespaces().removeMaxUnackedMessagesPerConsumer(namespace);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.namespaces().getMaxUnackedMessagesPerConsumer(namespace)));
        msgs = consumeMsg(consumer, 5);
        assertEquals(msgs.size(), 3);
        ackMsgs(consumer, msgs);

    }

    private List<Message> consumeMsg(Consumer<?> consumer, int msgNum) throws Exception {
        List<Message> list = new ArrayList<>();
        for (int i = 0; i <msgNum; i++) {
            Message message = consumer.receive(500, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            list.add(message);
        }
        return list;
    }

    private void ackMsgs(Consumer<?> consumer, List<Message> msgs) throws Exception {
        for (Message msg : msgs) {
            consumer.acknowledge(msg);
        }
    }
}
