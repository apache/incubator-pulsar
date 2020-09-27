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
package org.apache.pulsar.client.transaction;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckReplyCallBack;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckStoreState;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.transaction.common.exception.TransactionAckConflictException;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * End to end transaction test.
 */
@Slf4j
public class EndToEndTest extends TransactionTestBase {

    private final static int TOPIC_PARTITION = 3;

    private final static String TENANT = "tnx";
    private final static String NAMESPACE1 = TENANT + "/ns1";
    private final static String TOPIC_OUTPUT = NAMESPACE1 + "/output";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData("http://localhost:" + webServicePort));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT, TOPIC_PARTITION);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Thread.sleep(1000 * 3);
    }

    @AfterMethod
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void noBatchProduceCommitTest() throws Exception {
        produceCommitTest(false);
    }

    private void produceCommitTest(boolean enableBatch) throws Exception {
        List<String> list = admin.topics().getPartitionedTopicList(NAMESPACE1);
        @Cleanup
        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient
                .newConsumer()
                .topic(TOPIC_OUTPUT)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient
                .newProducer()
                .topic(TOPIC_OUTPUT)
                .enableBatching(enableBatch)
                .sendTimeout(0, TimeUnit.SECONDS);
        if (enableBatch) {
            producerBuilder.batcherBuilder(BatcherBuilder.KEY_BASED);
        }
        @Cleanup
        PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>) producerBuilder.create();

        Transaction txn1 = getTxn();
        Transaction txn2 = getTxn();

        int messageCnt = 20;
        for (int i = 0; i < messageCnt; i++) {
            if (i % 2 == 0) {
                producer.newMessage(txn1).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
            } else {
                producer.newMessage(txn2).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
            }
        }

        // Can't receive transaction messages before commit.
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        txn1.commit().get();

        // txn1 messages could be received after txn1 committed
        int receiveCnt = 0;
        for (int i = 0; i < messageCnt / 2; i++) {
            message = consumer.receive();
            Assert.assertNotNull(message);
            log.info("receive msgId: {}, msg: {}", message.getMessageId(), new String(message.getData(), UTF_8));
            receiveCnt ++;
        }
        Assert.assertEquals(messageCnt / 2, receiveCnt);

        message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        txn2.commit().get();

        // txn2 messages could be received after txn2 committed
        receiveCnt = 0;
        for (int i = 0; i < messageCnt / 2; i++) {
            message = consumer.receive();
            Assert.assertNotNull(message);
            log.info("receive second msgId: {}, msg: {}", message.getMessageId(), new String(message.getData(), UTF_8));
            receiveCnt ++;
        }
        Assert.assertEquals(messageCnt / 2, receiveCnt);

        message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        log.info("message commit test enableBatch {}", enableBatch);
    }

    @Test
    public void produceAbortTest() throws Exception {
        Transaction txn = getTxn();

        @Cleanup
        PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>) pulsarClient
                .newProducer()
                .topic(TOPIC_OUTPUT)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        int messageCnt = 10;
        for (int i = 0; i < messageCnt; i++) {
            producer.newMessage(txn).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
        }

        @Cleanup
        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient
                .newConsumer()
                .topic(TOPIC_OUTPUT)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        // Can't receive transaction messages before abort.
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        txn.abort().get();

        // Cant't receive transaction messages after abort.
        message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        log.info("finished test partitionAbortTest");
    }

    @Test
    public void txnAckTestNoBatchAndSharedSub() throws Exception {
        txnAckTest(false, 1, SubscriptionType.Shared);
    }

    @Test
    public void txnAckTestBatchAndSharedSub() throws Exception {
        txnAckTest(true, 5, SubscriptionType.Shared);
    }

    private void txnAckTest(boolean batchEnable, int maxBatchSize,
                         SubscriptionType subscriptionType) throws Exception {
        String normalTopic = NAMESPACE1 + "/normal-topic";

        @Cleanup
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(normalTopic)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(subscriptionType)
                .ackTimeout(1, TimeUnit.MINUTES)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(normalTopic)
                .enableBatching(batchEnable)
                .batchingMaxMessages(maxBatchSize)
                .create();

        for (int retryCnt = 0; retryCnt < 2; retryCnt++) {
            Transaction txn = getTxn();
            int messageCnt = 10;
            // produce normal messages
            for (int i = 0; i < messageCnt; i++){
                producer.newMessage().value("hello".getBytes()).sendAsync();
            }

            // consume and ack messages with txn
            for (int i = 0; i < messageCnt; i++) {
                Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                log.info("receive msgId abort: {}, retryCount : {}", message.getMessageId(), retryCnt);
                consumer.acknowledgeAsync(message.getMessageId(), txn).get();
            }
            Thread.sleep(2000);

            consumer.redeliverUnacknowledgedMessages();

            // the messages are pending ack state and can't be received
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);

            // 1) txn abort
            txn.abort().get();


            //TODO why redeliverUnacknowledgedMessages() can receive > messageCnt message.
//            consumer.redeliverUnacknowledgedMessages();

            Thread.sleep(2000);

            Transaction commitTxn = getTxn();
            for (int i = 0; i < messageCnt; i++) {
                message = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                if (i == 4) {
                    consumer.acknowledge(message);
                } else {
                    consumer.acknowledgeAsync(message.getMessageId(), commitTxn).get();
                }
                log.info("receive msgId commit: {}, retryCount : {} , count : {}", message.getMessageId(), retryCnt, i);
            }
            try {
                consumer.acknowledgeAsync(message.getMessageId(), commitTxn).get();
                fail("not ack conflict ");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof TransactionConflictException);
            }


            // 2) ack committed by a new txn
            commitTxn.commit().get();
            // after transaction commit, the messages can't be received
            message = consumer.receive(300, TimeUnit.MILLISECONDS);
            Assert.assertNull(message);

            try {
                commitTxn.commit().get();
                Assert.fail("recommit one transaction should be failed.");
            } catch (Exception reCommitError) {
                // recommit one transaction should be failed
                log.info("expected exception for recommit one transaction.");
                Assert.assertNotNull(reCommitError);
                Assert.assertTrue(reCommitError.getCause() instanceof
                        TransactionCoordinatorClientException.InvalidTxnStatusException);
            }
        }
    }

    @Test
    public void txnAckTestBatchAndCumulativeSub() throws Exception {
        txnCumulativeAckTest(true, 5, SubscriptionType.Failover);
    }

    @Test
    public void txnAckTestNoBatchAndCumulativeSub() throws Exception {
        txnCumulativeAckTest(false, 1, SubscriptionType.Failover);
    }

    public void txnCumulativeAckTest(boolean batchEnable, int maxBatchSize, SubscriptionType subscriptionType)
            throws Exception {
        String normalTopic = NAMESPACE1 + "/normal-topic";

        @Cleanup
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(normalTopic)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(subscriptionType)
                .ackTimeout(1, TimeUnit.MINUTES)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(normalTopic)
                .enableBatching(batchEnable)
                .batchingMaxMessages(maxBatchSize)
                .create();

        for (int retryCnt = 0; retryCnt < 2; retryCnt++) {
            Transaction abortTxn = getTxn();
            int messageCnt = 10;
            // produce normal messages
            for (int i = 0; i < messageCnt; i++){
                producer.newMessage().value("hello".getBytes()).sendAsync();
            }
            Message<byte[]> message = null;
            // consume and ack messages with txn
            List<Message<byte[]>> list = new ArrayList<>();
            for (int i = 0; i < messageCnt; i++) {
                message = consumer.receive(1, TimeUnit.SECONDS);
                list.add(message);
                Assert.assertNotNull(message);
                if (i % 3 == 0) {
                    consumer.acknowledgeCumulativeAsync(message.getMessageId(), abortTxn).get();
                }
                log.info("receive msgId abort: {}, retryCount : {}, count : {}", message.getMessageId(), retryCnt, i);
            }
            try {
                consumer.acknowledgeCumulativeAsync(message.getMessageId(), abortTxn).get();
                fail("not ack conflict ");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof TransactionConflictException);
            }
            abortTxn.abort().get();
            Transaction commitTxn = getTxn();
            for (int i = 0; i < messageCnt; i++) {
                if (i % 3 == 0) {
                    consumer.acknowledgeCumulativeAsync(list.get(i).getMessageId(), commitTxn).get();
                }
                log.info("receive msgId commit: {}, retryCount : {}, count : {}", message.getMessageId(), retryCnt, i);
            }
            commitTxn.commit().get();

            try {
                commitTxn.commit().get();
                Assert.fail("recommit one transaction should be failed.");
            } catch (Exception reCommitError) {
                // recommit one transaction should be failed
                log.info("expected exception for recommit one transaction.");
                Assert.assertNotNull(reCommitError);
                Assert.assertTrue(reCommitError.getCause() instanceof
                        TransactionCoordinatorClientException.InvalidTxnStatusException);
            }
        }
    }


    @Test
    public void txnMessageAckTest() throws Exception {
        final String subName = "test";
        @Cleanup
        MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient
                .newConsumer()
                .topic(TOPIC_OUTPUT)
                .subscriptionName(subName)
                .enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscribe();

        @Cleanup
        PartitionedProducerImpl<byte[]> producer = (PartitionedProducerImpl<byte[]>) pulsarClient
                .newProducer()
                .topic(TOPIC_OUTPUT)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        Transaction txn = getTxn();

        int messageCnt = 10;
        for (int i = 0; i < messageCnt; i++) {
            producer.newMessage(txn).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
        }
        log.info("produce transaction messages finished");

        // Can't receive transaction messages before commit.
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);
        log.info("transaction messages can't be received before transaction committed");

        txn.commit().get();

        Map<Integer, MessageIdImpl> messageIdMap = new HashMap<>();
        int ackedMessageCount = 0;
        int receiveCnt = 0;
        for (int i = 0; i < messageCnt; i++) {
            message = consumer.receive();
            if (i % 2 == 0) {
                consumer.acknowledge(message);
                ackedMessageCount ++;
            }
            Assert.assertNotNull(message);
            receiveCnt ++;

            MessageIdImpl messageId;
            if (message.getMessageId() instanceof TopicMessageIdImpl) {
                messageId = (MessageIdImpl) ((TopicMessageIdImpl) message.getMessageId()).getInnerMessageId();
            } else {
                messageId = (MessageIdImpl) message.getMessageId();
            }
            messageIdMap.put(messageId.getPartitionIndex(), messageId);
        }
        Assert.assertEquals(messageCnt, receiveCnt);

        for (int i = 0; i < TOPIC_PARTITION; i++) {
            Assert.assertEquals(
                    messageIdMap.get(i).getLedgerId() + ":-1",
                    getMarkDeletePosition(TOPIC_OUTPUT, i, subName));
        }

        consumer.redeliverUnacknowledgedMessages();

        receiveCnt = 0;
        for (int i = 0; i < messageCnt - ackedMessageCount; i++) {
            message = consumer.receive(2, TimeUnit.SECONDS);
            log.info("second receive messageId: {}", message.getMessageId());
            Assert.assertNotNull(message);
            consumer.acknowledge(message);
            receiveCnt ++;
        }
        Assert.assertEquals(messageCnt - ackedMessageCount, receiveCnt);

        message = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);

        for (int i = 0; i < TOPIC_PARTITION; i++) {
            Assert.assertEquals(
                    messageIdMap.get(i).getLedgerId() + ":" + messageIdMap.get(i).getEntryId(),
                    getMarkDeletePosition(TOPIC_OUTPUT, i, subName));
        }

        log.info("receive transaction messages count: {}", receiveCnt);
    }

    @Test
    public void testIndividualPendingAckPersistAndReply() throws ExecutionException, InterruptedException {
        String topicName = "test";
        String subName = "test";
        MLPendingAckStore mlPendingAckStore =
                new MLPendingAckStore(getPulsarServiceList().get(1).getManagedLedgerFactory(), topicName, subName);
        PersistentSubscription persistentSubscription = Mockito.mock(PersistentSubscription.class);
        CompletableFuture<Void> completableFuture = mock(CompletableFuture.class);
        doAnswer(invocation -> invocation.callRealMethod())
                .when(persistentSubscription).handleMetadataEntry(any(), any(), any());
        doReturn(null).when(completableFuture).get();
        mlPendingAckStore.replayAsync(new MLPendingAckReplyCallBack(mlPendingAckStore, persistentSubscription));
        doReturn(topicName).when(persistentSubscription).getTopicName();
        doReturn(subName).when(persistentSubscription).getName();
        TxnID txnIDOne = new TxnID(1, 1);
        TxnID txnIDTwo = new TxnID(1, 2);
        PositionImpl positionOne = PositionImpl.get(1, 1);
        PositionImpl positionTwo = PositionImpl.get(1, 2);
        PositionImpl positionThree = PositionImpl.get(2, 1);
        int pendingCount = 5;
        while (mlPendingAckStore.getState() != PendingAckStoreState.State.Ready && pendingCount > 1) {
            Thread.sleep(1000L);
            pendingCount--;
        }
        try {
            mlPendingAckStore.append(txnIDOne, positionOne, AckType.Individual).get();
            mlPendingAckStore.append(txnIDOne, positionTwo, AckType.Individual).get();
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            mlPendingAckStore.deleteTxn(txnIDTwo, AckType.Individual).get();
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof TransactionAckConflictException);
        }

        try {
            mlPendingAckStore.append(txnIDTwo, positionThree, AckType.Individual).get();
        } catch (Exception e) {
            Assert.fail();
        }
        mlPendingAckStore =
                new MLPendingAckStore(getPulsarServiceList().get(1).getManagedLedgerFactory(), topicName, subName);
        mlPendingAckStore.replayAsync(new MLPendingAckReplyCallBack(mlPendingAckStore, persistentSubscription));
        while (mlPendingAckStore.getState() != PendingAckStoreState.State.Ready && pendingCount > 1) {
            Thread.sleep(1000L);
            pendingCount--;
        }
        if (mlPendingAckStore.getState() != PendingAckStoreState.State.Ready) {
            Assert.fail();
        }
        try {
            mlPendingAckStore.deleteTxn(txnIDOne, AckType.Individual).get();
            mlPendingAckStore.deleteTxn(txnIDTwo, AckType.Individual).get();
        } catch (ExecutionException e) {
            Assert.fail();
        }
    }

    @Test
    public void testCumulativePendingAckPersistAndReply() throws InterruptedException, ExecutionException {
        String topicName = "test";
        String subName = "test";
        MLPendingAckStore mlPendingAckStore =
                new MLPendingAckStore(getPulsarServiceList().get(1).getManagedLedgerFactory(), topicName, subName);
        PersistentSubscription persistentSubscription = Mockito.mock(PersistentSubscription.class);
        CompletableFuture<Void> completableFuture = mock(CompletableFuture.class);
        doAnswer(invocation -> invocation.callRealMethod())
                .when(persistentSubscription).handleMetadataEntry(any(), any(), any());
        doReturn(null).when(completableFuture).get();
        mlPendingAckStore.replayAsync(new MLPendingAckReplyCallBack(mlPendingAckStore, persistentSubscription));
        doReturn(topicName).when(persistentSubscription).getTopicName();
        doReturn(subName).when(persistentSubscription).getName();
        TxnID txnIDOne = new TxnID(1, 1);
        TxnID txnIDTwo = new TxnID(1, 2);
        PositionImpl position = PositionImpl.get(1, 1);
        int pendingCount = 5;
        while (mlPendingAckStore.getState() != PendingAckStoreState.State.Ready && pendingCount > 1) {
            Thread.sleep(1000L);
            pendingCount--;
        }
        try {
            mlPendingAckStore.append(txnIDOne, position, AckType.Cumulative).get();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            mlPendingAckStore.deleteTxn(txnIDTwo, AckType.Cumulative).get();
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof TransactionAckConflictException);
        }
        mlPendingAckStore =
                new MLPendingAckStore(getPulsarServiceList().get(1).getManagedLedgerFactory(), topicName, subName);
        mlPendingAckStore.replayAsync(new MLPendingAckReplyCallBack(mlPendingAckStore, persistentSubscription));
        while (mlPendingAckStore.getState() != PendingAckStoreState.State.Ready && pendingCount > 1) {
            Thread.sleep(1000L);
            pendingCount--;
        }
        if (mlPendingAckStore.getState() != PendingAckStoreState.State.Ready) {
            Assert.fail();
        }
        try {
            mlPendingAckStore.deleteTxn(txnIDOne, AckType.Cumulative).get();
        } catch (ExecutionException e) {
            Assert.fail();
        }
    }

    private Transaction getTxn() throws Exception {
        return ((PulsarClientImpl) pulsarClient)
                .newTransaction()
                .withTransactionTimeout(2, TimeUnit.SECONDS)
                .build()
                .get();
    }

    private String getMarkDeletePosition(String topic, Integer partition, String subName) throws Exception {
        topic = TopicName.get(topic).getPartition(partition).toString();
        PersistentTopicInternalStats stats = admin.topics().getInternalStats(topic);
        return stats.cursors.get(subName).markDeletePosition;
    }

}
