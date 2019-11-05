package org.apache.pulsar.transaction.coordinator.impl;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;

class TopicBaseTransactionReaderImpl implements
        TopicBaseTransactionMetadataStore.TopicBaseTransactionReader {

    private ConcurrentMap<TxnID, TxnMeta> txnMetaMap = new ConcurrentHashMap<>();

    private long sequenceId;

    private final ManagedLedgerFactory managedLedgerFactory;

    public TopicBaseTransactionReaderImpl(String tcId, ManagedLedgerFactory managedLedgerFactory) throws Exception {
        this.managedLedgerFactory =  managedLedgerFactory;
            ReadOnlyCursor readOnlyCursor = managedLedgerFactory
                    .openReadOnlyCursor(tcId,
                            PositionImpl.earliest, new ManagedLedgerConfig());
        while (readOnlyCursor.hasMoreEntries()) {
            List<Entry> entries = readOnlyCursor.readEntries(100);
            for (int i = 0; i < entries.size(); i++) {
                ByteBuf buffer = entries.get(i).getDataBuffer();
                ByteBufCodedInputStream stream = ByteBufCodedInputStream.get(buffer);
                TransactionMetadataEntry.Builder transactionMetadataEntryBuilder =
                        TransactionMetadataEntry.newBuilder();
                TransactionMetadataEntry transactionMetadataEntry =
                        transactionMetadataEntryBuilder.mergeFrom(stream, null).build();
                TxnID txnID = new TxnID(transactionMetadataEntry.getTxnidMostBits(),
                        transactionMetadataEntry.getTxnidLeastBits());
                switch (transactionMetadataEntry.getMetadataOp()) {
                    case NEW:
                        sequenceId = sequenceId > transactionMetadataEntry.getTxnidLeastBits()
                                ? sequenceId : transactionMetadataEntry.getTxnidMostBits();
                        txnMetaMap.put(txnID, new TxnMetaImpl(txnID));
                        transactionMetadataEntryBuilder.recycle();
                        stream.recycle();
                        break;
                    case ADD_PARTITION:
                        txnMetaMap.get(txnID).addProducedPartitions(transactionMetadataEntry.getPartitionsList());
                        transactionMetadataEntryBuilder.recycle();
                        stream.recycle();
                        break;
                    case ADD_SUBSCRIPTION:
                        txnMetaMap.get(txnID)
                                .addTxnSubscription(
                                        subscriptionToTxnSubscription(transactionMetadataEntry.getSubscriptionsList()));
                        transactionMetadataEntryBuilder.recycle();
                        stream.recycle();
                        break;
                    case UPDATE:
                        txnMetaMap.get(txnID)
                                .updateTxnStatus(transactionMetadataEntry.getNewStatus(),
                                        transactionMetadataEntry.getExpectedStatus());
                        transactionMetadataEntryBuilder.recycle();
                        stream.recycle();
                        break;
                    default:
                        throw new InvalidTxnStatusException("Transaction `"
                                + txnID + "` load bad metadata operation from transaction log ");

                }
                entries.get(i).release();
            }
        }
    }

    private static List<TxnSubscription> subscriptionToTxnSubscription(List<Subscription> subscriptions) {
        List<TxnSubscription> txnSubscriptions = new ArrayList<>(subscriptions.size());
        for (int i = 0; i < subscriptions.size(); i++) {
            txnSubscriptions
                    .add(new TxnSubscription(subscriptions.get(i).getTopic(), subscriptions.get(i).getSubscription()));
        }
        return txnSubscriptions;
    }


    @Override
    public TxnMeta getTxnMeta(TxnID txnid) {
        return txnMetaMap.get(txnid);
    }

    @Override
    public Long readSequenceId() {
        return sequenceId;
    }

    @Override
    public void addNewTxn(TxnMeta txnMeta) {
        txnMetaMap.put(txnMeta.id(), txnMeta);
    }

    @Override
    public TxnStatus getTxnStatus(TxnID txnID) {
        return txnMetaMap.get(txnID).status();
    }


}
