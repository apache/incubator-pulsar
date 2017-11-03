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
package org.apache.pulsar.storm;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class PulsarBoltTest extends ProducerConsumerBase {

    private static final int NO_OF_RETRIES = 10;

    public final String serviceUrl = "http://127.0.0.1:" + BROKER_WEBSERVICE_PORT;
    public final String topic = "persistent://my-property/use/my-ns/my-topic1";
    public final String subscriptionName = "my-subscriber-name";

    protected PulsarBoltConfiguration pulsarBoltConf;
    protected PulsarBolt bolt;
    protected MockOutputCollector mockCollector;
    protected Consumer consumer;

    @Override
    @BeforeMethod
    public void beforeMethod(Method m) throws Exception {
        super.beforeMethod(m);
        setup();
    }

    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        pulsarBoltConf = new PulsarBoltConfiguration();
        pulsarBoltConf.setServiceUrl(serviceUrl);
        pulsarBoltConf.setTopic(topic);
        pulsarBoltConf.setTupleToMessageMapper(tupleToMessageMapper);
        pulsarBoltConf.setMetricsTimeIntervalInSecs(60);
        bolt = new PulsarBolt(pulsarBoltConf, new ClientConfiguration());
        mockCollector = new MockOutputCollector();
        OutputCollector collector = new OutputCollector(mockCollector);
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("test-bolt-" + methodName);
        when(context.getThisTaskId()).thenReturn(0);
        bolt.prepare(Maps.newHashMap(), context, collector);
        consumer = pulsarClient.subscribe(topic, subscriptionName);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        bolt.close();
        consumer.close();
        super.internalCleanup();
    }

    @SuppressWarnings("serial")
    static TupleToMessageMapper tupleToMessageMapper = new TupleToMessageMapper() {

        @Override
        public Message toMessage(Tuple tuple) {
            if ("message to be dropped".equals(new String(tuple.getBinary(0)))) {
                return null;
            }
            if ("throw exception".equals(new String(tuple.getBinary(0)))) {
                throw new RuntimeException();
            }
            return MessageBuilder.create().setContent(tuple.getBinary(0)).build();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    };

    private Tuple getMockTuple(String msgContent) {
        Tuple mockTuple = mock(Tuple.class);
        when(mockTuple.getBinary(0)).thenReturn(msgContent.getBytes());
        when(mockTuple.getSourceComponent()).thenReturn("");
        when(mockTuple.getSourceStreamId()).thenReturn("");
        return mockTuple;
    }

    @Test
    public void testBasic() throws Exception {
        String msgContent = "hello world";
        Tuple tuple = getMockTuple(msgContent);
        bolt.execute(tuple);
        for (int i = 0; i < NO_OF_RETRIES; i++) {
            Thread.sleep(1000);
            if (mockCollector.acked()) {
                break;
            }
        }
        Assert.assertTrue(mockCollector.acked());
        Assert.assertFalse(mockCollector.failed());
        Assert.assertNull(mockCollector.getLastError());
        Assert.assertEquals(tuple, mockCollector.getAckedTuple());
        Message msg = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledge(msg);
        Assert.assertEquals(msgContent, new String(msg.getData()));
    }

    @Test
    public void testExecuteFailure() throws Exception {
        String msgContent = "throw exception";
        Tuple tuple = getMockTuple(msgContent);
        bolt.execute(tuple);
        Assert.assertFalse(mockCollector.acked());
        Assert.assertTrue(mockCollector.failed());
        Assert.assertNotNull(mockCollector.getLastError());
    }

    @Test
    public void testNoMessageSend() throws Exception {
        String msgContent = "message to be dropped";
        Tuple tuple = getMockTuple(msgContent);
        bolt.execute(tuple);
        Assert.assertTrue(mockCollector.acked());
        Message msg = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(msg);
    }

    @Test
    public void testMetrics() throws Exception {
        bolt.resetMetrics();
        String msgContent = "hello world";
        Tuple tuple = getMockTuple(msgContent);
        for (int i = 0; i < 10; i++) {
            bolt.execute(tuple);
        }
        for (int i = 0; i < NO_OF_RETRIES; i++) {
            Thread.sleep(1000);
            if (mockCollector.getNumTuplesAcked() == 10) {
                break;
            }
        }
        @SuppressWarnings("rawtypes")
        Map metrics = (Map) bolt.getValueAndReset();
        Assert.assertEquals(((Long) metrics.get(PulsarBolt.NO_OF_MESSAGES_SENT)).longValue(), 10);
        Assert.assertEquals(((Double) metrics.get(PulsarBolt.PRODUCER_RATE)).doubleValue(),
                10.0 / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        Assert.assertEquals(((Double) metrics.get(PulsarBolt.PRODUCER_THROUGHPUT_BYTES)).doubleValue(),
                ((double) msgContent.getBytes().length * 10) / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        metrics = bolt.getMetrics();
        Assert.assertEquals(((Long) metrics.get(PulsarBolt.NO_OF_MESSAGES_SENT)).longValue(), 0);
        for (int i = 0; i < 10; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }
    }

    @Test
    public void testSharedProducer() throws Exception {
        PersistentTopicStats topicStats = admin.persistentTopics().getStats(topic);
        Assert.assertEquals(topicStats.publishers.size(), 1);
        PulsarBolt otherBolt = new PulsarBolt(pulsarBoltConf, new ClientConfiguration());
        MockOutputCollector otherMockCollector = new MockOutputCollector();
        OutputCollector collector = new OutputCollector(otherMockCollector);
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("test-bolt-" + methodName);
        when(context.getThisTaskId()).thenReturn(1);
        otherBolt.prepare(Maps.newHashMap(), context, collector);

        topicStats = admin.persistentTopics().getStats(topic);
        Assert.assertEquals(topicStats.publishers.size(), 1);

        otherBolt.close();

        topicStats = admin.persistentTopics().getStats(topic);
        Assert.assertEquals(topicStats.publishers.size(), 1);
    }

    @Test
    public void testSerializability() throws Exception {
        // test serializability with no auth
        PulsarBolt boltWithNoAuth = new PulsarBolt(pulsarBoltConf, new ClientConfiguration());
        TestUtil.testSerializability(boltWithNoAuth);
    }

    @Test
    public void testTickTuple() throws Exception {
        Tuple mockTuple = mock(Tuple.class);
        when(mockTuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);
        when(mockTuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        bolt.execute(mockTuple);
        Assert.assertTrue(mockCollector.acked());
        Message msg = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(msg);
    }
}
