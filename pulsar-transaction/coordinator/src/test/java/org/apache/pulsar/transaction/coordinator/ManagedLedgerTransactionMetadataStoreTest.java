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
package org.apache.pulsar.transaction.coordinator;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.transaction.coordinator.impl.ManagedLedgerTransactionMetadataStore;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class ManagedLedgerTransactionMetadataStoreTest extends BookKeeperClusterTestCase {

    public ManagedLedgerTransactionMetadataStoreTest() {
        super(3);
    }

    @Test
    public void testTransactionOperation() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc, factoryConf);
        ManagedLedgerTransactionMetadataStore transactionMetadataStore =
                new ManagedLedgerTransactionMetadataStore(new TransactionCoordinatorID(1), factory);

        while (true) {
            if (transactionMetadataStore.checkIfReady()) {
                TxnID txnID = transactionMetadataStore.newTransactionAsync(1000).get();
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.OPEN);

                List<String> partitions = new ArrayList<>();
                partitions.add("pt-1");
                partitions.add("pt-2");
                transactionMetadataStore.addProducedPartitionToTxnAsync(txnID, partitions).get();
                Assert.assertEquals(transactionMetadataStore.getTxnMetaAsync(txnID).get().producedPartitions(), partitions);

                partitions.add("pt-3");
                transactionMetadataStore.addProducedPartitionToTxnAsync(txnID, partitions).get();
                Assert.assertEquals(transactionMetadataStore.getTxnMetaAsync(txnID).get().producedPartitions(),
                        partitions);

                List<TxnSubscription> subscriptions = new ArrayList<>();
                subscriptions.add(new TxnSubscription("topic1", "sub1"));
                subscriptions.add(new TxnSubscription("topic2", "sub2"));
                transactionMetadataStore.addAckedPartitionToTxnAsync(txnID, subscriptions).get();
                Assert.assertTrue(transactionMetadataStore.getTxnMetaAsync(txnID).get().ackedPartitions().containsAll(subscriptions));

                transactionMetadataStore.addAckedPartitionToTxnAsync(txnID, subscriptions).get();
                Assert.assertEquals(transactionMetadataStore.getTxnMetaAsync(txnID).get().producedPartitions(),
                        partitions);

                transactionMetadataStore.updateTxnStatusAsync(txnID, TxnStatus.COMMITTING, TxnStatus.OPEN).get();
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.COMMITTING);

                transactionMetadataStore.updateTxnStatusAsync(txnID, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.COMMITTED);
                break;
            } else {
                Thread.sleep(100);
            }
        }
    }

    @Test
    public void testInitTransactionReader() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc, factoryConf);
        ManagedLedgerTransactionMetadataStore transactionMetadataStore =
                new ManagedLedgerTransactionMetadataStore(new TransactionCoordinatorID(1), factory);

        while (true) {
            if (transactionMetadataStore.checkIfReady()) {
                TxnID txnID1 = transactionMetadataStore.newTransactionAsync(1000).get();
                TxnID txnID2 = transactionMetadataStore.newTransactionAsync(1000).get();
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID1).get(), TxnStatus.OPEN);
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID2).get(), TxnStatus.OPEN);

                List<String> partitions = new ArrayList<>();
                partitions.add("pt-1");
                partitions.add("pt-2");
                transactionMetadataStore.addProducedPartitionToTxnAsync(txnID1, partitions).get();
                transactionMetadataStore.addProducedPartitionToTxnAsync(txnID2, partitions).get();

                List<TxnSubscription> subscriptions = new ArrayList<>();
                subscriptions.add(new TxnSubscription("topic1", "sub1"));
                subscriptions.add(new TxnSubscription("topic2", "sub2"));

                transactionMetadataStore.addAckedPartitionToTxnAsync(txnID1, subscriptions).get();
                transactionMetadataStore.addAckedPartitionToTxnAsync(txnID2, subscriptions).get();
                List<TxnSubscription> subscriptions1 = new ArrayList<>();
                subscriptions1.add(new TxnSubscription("topic3", "sub3"));
                subscriptions1.add(new TxnSubscription("topic3", "sub3"));
                transactionMetadataStore.addAckedPartitionToTxnAsync(txnID1, subscriptions1).get();
                transactionMetadataStore.addAckedPartitionToTxnAsync(txnID2, subscriptions1).get();

                transactionMetadataStore.updateTxnStatusAsync(txnID1, TxnStatus.COMMITTING, TxnStatus.OPEN).get();
                transactionMetadataStore.updateTxnStatusAsync(txnID2, TxnStatus.COMMITTING, TxnStatus.OPEN).get();

                transactionMetadataStore.updateTxnStatusAsync(txnID1, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();
                transactionMetadataStore.updateTxnStatusAsync(txnID2, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();
                transactionMetadataStore.closeAsync();
                ManagedLedgerTransactionMetadataStore transactionMetadataStoreTest =
                        new ManagedLedgerTransactionMetadataStore(new TransactionCoordinatorID(1), factory);

                while (true) {
                    if (transactionMetadataStoreTest.checkIfReady()) {
                        TxnMeta txnMeta1 = transactionMetadataStoreTest.getTxnMetaAsync(txnID1).get();
                        TxnMeta txnMeta2 = transactionMetadataStoreTest.getTxnMetaAsync(txnID2).get();
                        Assert.assertEquals(txnMeta1.producedPartitions(), partitions);
                        Assert.assertEquals(txnMeta2.producedPartitions(), partitions);
                        Assert.assertEquals(txnMeta1.ackedPartitions().size(), transactionMetadataStore.getTxnMetaAsync(txnID1).get().ackedPartitions().size());
                        Assert.assertEquals(txnMeta2.ackedPartitions().size(), transactionMetadataStore.getTxnMetaAsync(txnID2).get().ackedPartitions().size());
                        Assert.assertTrue(transactionMetadataStore.getTxnMetaAsync(txnID1).get().ackedPartitions().containsAll(txnMeta1.ackedPartitions()));
                        Assert.assertTrue(transactionMetadataStore.getTxnMetaAsync(txnID2).get().ackedPartitions().containsAll(txnMeta2.ackedPartitions()));
                        Assert.assertEquals(txnMeta1.status(), TxnStatus.COMMITTED);
                        Assert.assertEquals(txnMeta2.status(), TxnStatus.COMMITTED);
                        TxnID txnID = transactionMetadataStoreTest.newTransactionAsync(1000).get();
                        Assert.assertEquals(txnID.getLeastSigBits(), 2L);
                        break;
                    } else {
                        Thread.sleep(100);
                    }
                }
                break;
            } else {
                Thread.sleep(100);
            }
        }

    }
}
