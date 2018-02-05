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

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class PatternTopicsConsumerImplTest extends ProducerConsumerBase {
    private static final long testTimeout = 90000; // 1.5 min
    private static final Logger log = LoggerFactory.getLogger(PatternTopicsConsumerImplTest.class);
    private final long ackTimeOutMillis = TimeUnit.SECONDS.toMillis(2);

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        // set isTcpLookup = true, to use BinaryProtoLookupService to get topics for a pattern.
        isTcpLookup = true;
        super.internalSetup();
    }

    @Override
    @AfterMethod
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    // verify consumer create success, and works well.
    @Test(timeOut = testTimeout)
    public void testBinaryProtoToGetTopicsOfNamespace() throws Exception {
        String key = "BinaryProtoToGetTopics";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://prop/use/ns-abc/pattern-topic-1-" + key;
        String topicName2 = "persistent://prop/use/ns-abc/pattern-topic-2-" + key;
        String topicName3 = "persistent://prop/use/ns-abc/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("persistent://prop/use/ns-abc/pattern-topic.*");

        // 1. create partition
        admin.properties().createProperty("prop", new PropertyAdmin());
        admin.persistentTopics().createPartitionedTopic(topicName2, 2);
        admin.persistentTopics().createPartitionedTopic(topicName3, 3);

        // 2. create producer
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer producer1 = pulsarClient.createProducer(topicName1);
        Producer producer2 = pulsarClient.createProducer(topicName2, producerConfiguration);
        Producer producer3 = pulsarClient.createProducer(topicName3, producerConfiguration);

        // 3. Create consumer, this should success
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(4);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribeAsync(pattern, subscriptionName, conf).get();
        assertTrue(consumer instanceof PatternTopicsConsumerImpl);

        // 4. verify consumer get methods, to get right number of partitions and topics.
        assertSame(pattern, ((PatternTopicsConsumerImpl) consumer).getPattern());
        List<String> topics = ((PatternTopicsConsumerImpl) consumer).getPartitionedTopics();
        List<ConsumerImpl> consumers = ((PatternTopicsConsumerImpl) consumer).getConsumers();

        assertEquals(topics.size(), 6);
        assertEquals(consumers.size(), 6);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getTopics().size(), 3);

        topics.forEach(topic -> log.debug("topic: {}", topic));
        consumers.forEach(c -> log.debug("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
            assertTrue(topics.get(index).equals(consumers.get(index).getTopic())));

        ((PatternTopicsConsumerImpl) consumer).getTopics().forEach(topic -> log.debug("getTopics topic: {}", topic));

        // 5. produce data
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        // 6. should receive all the message
        int messageSet = 0;
        Message message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
    }

    @Test(timeOut = testTimeout)
    public void testTopicsPatternFilter() throws Exception {
        String topicName1 = "persistent://prop/use/ns-abc/pattern-topic-1";
        String topicName2 = "persistent://prop/use/ns-abc/pattern-topic-2";
        String topicName3 = "persistent://prop/use/ns-abc/hello-3";

        List<String> topicsNames = Lists.newArrayList(topicName1, topicName2, topicName3);

        Pattern pattern1 = Pattern.compile("persistent://prop/use/ns-abc/pattern-topic.*");
        List<String> result1 = PulsarClientImpl.topicsPatternFilter(topicsNames, pattern1);
        assertTrue(result1.size() == 2 && result1.contains(topicName1) && result1.contains(topicName2));

        Pattern pattern2 = Pattern.compile("persistent://prop/use/ns-abc/.*");
        List<String> result2 = PulsarClientImpl.topicsPatternFilter(topicsNames, pattern2);
        assertTrue(result2.size() == 3 &&
            result2.contains(topicName1) &&
            result2.contains(topicName2) &&
            result2.contains(topicName3));
    }

    @Test(timeOut = testTimeout)
    public void testTopicsListMinus() throws Exception {
        String topicName1 = "persistent://prop/use/ns-abc/pattern-topic-1";
        String topicName2 = "persistent://prop/use/ns-abc/pattern-topic-2";
        String topicName3 = "persistent://prop/use/ns-abc/pattern-topic-3";
        String topicName4 = "persistent://prop/use/ns-abc/pattern-topic-4";
        String topicName5 = "persistent://prop/use/ns-abc/pattern-topic-5";
        String topicName6 = "persistent://prop/use/ns-abc/pattern-topic-6";

        List<String> oldNames = Lists.newArrayList(topicName1, topicName2, topicName3, topicName4);
        List<String> newNames = Lists.newArrayList(topicName3, topicName4, topicName5, topicName6);

        List<String> addedNames = PatternTopicsConsumerImpl.topicsListsMinus(newNames, oldNames);
        List<String> removedNames = PatternTopicsConsumerImpl.topicsListsMinus(oldNames, newNames);

        assertTrue(addedNames.size() == 2 &&
            addedNames.contains(topicName5) &&
            addedNames.contains(topicName6));
        assertTrue(removedNames.size() == 2 &&
            removedNames.contains(topicName1) &&
            removedNames.contains(topicName2));

        // totally 2 different list, should return content of first lists.
        List<String> addedNames2 = PatternTopicsConsumerImpl.topicsListsMinus(addedNames, removedNames);
        assertTrue(addedNames2.size() == 2 &&
            addedNames2.contains(topicName5) &&
            addedNames2.contains(topicName6));

        // 2 same list, should return empty list.
        List<String> addedNames3 = PatternTopicsConsumerImpl.topicsListsMinus(addedNames, addedNames);
        assertEquals(addedNames3.size(), 0);

        // empty list minus: addedNames2.size = 2, addedNames3.size = 0
        List<String> addedNames4 = PatternTopicsConsumerImpl.topicsListsMinus(addedNames2, addedNames3);
        assertTrue(addedNames4.size() == addedNames2.size());
        addedNames4.forEach(name -> assertTrue(addedNames2.contains(name)));

        List<String> addedNames5 = PatternTopicsConsumerImpl.topicsListsMinus(addedNames3, addedNames2);
        assertEquals(addedNames5.size(), 0);
    }

    // simulate subscribe a pattern which has no topics, but then matched topics added in.
    @Test(timeOut = testTimeout)
    public void testStartEmptyPatternConsumer() throws Exception {
        String key = "StartEmptyPatternConsumerTest";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://prop/use/ns-abc/pattern-topic-1-" + key;
        String topicName2 = "persistent://prop/use/ns-abc/pattern-topic-2-" + key;
        String topicName3 = "persistent://prop/use/ns-abc/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("persistent://prop/use/ns-abc/pattern-topic.*");

        // 1. create partition
        admin.properties().createProperty("prop", new PropertyAdmin());
        admin.persistentTopics().createPartitionedTopic(topicName2, 2);
        admin.persistentTopics().createPartitionedTopic(topicName3, 3);

        // 2. Create consumer, this should success, but with empty sub-consumser internal
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(4);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribeAsync(pattern, subscriptionName, conf).get();
        assertTrue(consumer instanceof PatternTopicsConsumerImpl);

        // 3. verify consumer get methods, to get 0 number of partitions and topics.
        assertSame(pattern, ((PatternTopicsConsumerImpl) consumer).getPattern());
        assertEquals(((PatternTopicsConsumerImpl) consumer).getPartitionedTopics().size(), 0);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getConsumers().size(), 0);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getTopics().size(), 0);

        // 4. create producer
        log.debug("create producers");
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer producer1 = pulsarClient.createProducer(topicName1);
        Producer producer2 = pulsarClient.createProducer(topicName2, producerConfiguration);
        Producer producer3 = pulsarClient.createProducer(topicName3, producerConfiguration);

        // 5. call recheckTopics to subscribe each added topics above
        log.debug("recheck topics change");
        ((PatternTopicsConsumerImpl) consumer).recheckTopics().get();

        // 6. verify consumer get methods, to get number of partitions and topics, value 6=1+2+3.
        assertSame(pattern, ((PatternTopicsConsumerImpl) consumer).getPattern());
        assertEquals(((PatternTopicsConsumerImpl) consumer).getPartitionedTopics().size(), 6);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getConsumers().size(), 6);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getTopics().size(), 3);


        // 7. produce data
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        // 8. should receive all the message
        int messageSet = 0;
        Message message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
    }

    @Test(timeOut = testTimeout)
    public void testAutoSubscribePatternConsumer() throws Exception {
        String key = "AutoSubscribePatternConsumer";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://prop/use/ns-abc/pattern-topic-1-" + key;
        String topicName2 = "persistent://prop/use/ns-abc/pattern-topic-2-" + key;
        String topicName3 = "persistent://prop/use/ns-abc/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("persistent://prop/use/ns-abc/pattern-topic.*");

        // 1. create partition
        admin.properties().createProperty("prop", new PropertyAdmin());
        admin.persistentTopics().createPartitionedTopic(topicName2, 2);
        admin.persistentTopics().createPartitionedTopic(topicName3, 3);

        // 2. create producer
        log.debug("create producers");
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer producer1 = pulsarClient.createProducer(topicName1);
        Producer producer2 = pulsarClient.createProducer(topicName2, producerConfiguration);
        Producer producer3 = pulsarClient.createProducer(topicName3, producerConfiguration);

        // 3. Create consumer, verify consumer get methods, to get 6 of partitions and topics: 6=1+2+3
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(4);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribeAsync(pattern, subscriptionName, conf).get();
        assertTrue(consumer instanceof PatternTopicsConsumerImpl);

        // 4. verify consumer get methods, to get 6 number of partitions and topics: 6=1+2+3
        assertSame(pattern, ((PatternTopicsConsumerImpl) consumer).getPattern());
        assertEquals(((PatternTopicsConsumerImpl) consumer).getPartitionedTopics().size(), 6);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getConsumers().size(), 6);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getTopics().size(), 3);

        // 5. produce data to topic 1,2,3; verify should receive all the message
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        int messageSet = 0;
        Message message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        // 6. create another producer with 4 partitions
        String topicName4 = "persistent://prop/use/ns-abc/pattern-topic-4-" + key;
        admin.persistentTopics().createPartitionedTopic(topicName4, 4);
        Producer producer4 = pulsarClient.createProducer(topicName4, producerConfiguration);

        // 7. call recheckTopics to subscribe each added topics above, verify topics number: 10=1+2+3+4
        log.debug("recheck topics change");
        ((PatternTopicsConsumerImpl) consumer).recheckTopics().get();
        assertEquals(((PatternTopicsConsumerImpl) consumer).getPartitionedTopics().size(), 10);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getConsumers().size(), 10);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getTopics().size(), 4);

        // 8. produce data to topic3 and topic4, verify should receive all the message
        for (int i = 0; i < totalMessages / 2; i++) {
            producer3.send((messagePredicate + "round2-producer4-" + i).getBytes());
            producer4.send((messagePredicate + "round2-producer4-" + i).getBytes());
        }

        messageSet = 0;
        message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
        producer4.close();
    }

    @Test(timeOut = testTimeout)
    public void testAutoUnbubscribePatternConsumer() throws Exception {
        String key = "AutoUnsubscribePatternConsumer";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://prop/use/ns-abc/pattern-topic-1-" + key;
        String topicName2 = "persistent://prop/use/ns-abc/pattern-topic-2-" + key;
        String topicName3 = "persistent://prop/use/ns-abc/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("persistent://prop/use/ns-abc/pattern-topic.*");

        // 1. create partition
        admin.properties().createProperty("prop", new PropertyAdmin());
        admin.persistentTopics().createPartitionedTopic(topicName2, 2);
        admin.persistentTopics().createPartitionedTopic(topicName3, 3);

        // 2. create producer
        log.debug("create producers");
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer producer1 = pulsarClient.createProducer(topicName1);
        Producer producer2 = pulsarClient.createProducer(topicName2, producerConfiguration);
        Producer producer3 = pulsarClient.createProducer(topicName3, producerConfiguration);

        // 3. Create consumer, this should success, but with empty sub-consumser internal
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(4);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribeAsync(pattern, subscriptionName, conf).get();
        assertTrue(consumer instanceof PatternTopicsConsumerImpl);

        // 4. verify consumer get methods, to get 0 number of partitions and topics: 6=1+2+3
        assertSame(pattern, ((PatternTopicsConsumerImpl) consumer).getPattern());
        assertEquals(((PatternTopicsConsumerImpl) consumer).getPartitionedTopics().size(), 6);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getConsumers().size(), 6);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getTopics().size(), 3);


        // 5. produce data to topic 1,2,3; verify should receive all the message
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        int messageSet = 0;
        Message message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);


        // 6. remove producer 1,3; verify only consumer 2 left
        // seems no direct way to verify auto-unsubscribe, because this patternConsumer also referenced the topic.
        List<String> topicNames = Lists.newArrayList(topicName2);
        NamespaceService nss = pulsar.getNamespaceService();
        doReturn(topicNames).when(nss).getListOfDestinations("prop", "use", "ns-abc");

        // 7. call recheckTopics to unsubscribe topic 1,3 , verify topics number: 2=6-1-3
        log.debug("recheck topics change");
        ((PatternTopicsConsumerImpl) consumer).recheckTopics().get();
        assertEquals(((PatternTopicsConsumerImpl) consumer).getPartitionedTopics().size(), 2);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getConsumers().size(), 2);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getTopics().size(), 1);


        // 8. produce data to topic2, verify should receive all the message
        for (int i = 0; i < totalMessages; i++) {
            producer2.send((messagePredicate + "round2-producer2-" + i).getBytes());
        }

        messageSet = 0;
        message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
    }
}
