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
package org.apache.pulsar.broker.transaction.buffer.impl;

import com.google.common.annotations.VisibleForTesting;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Sets;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.broker.transaction.buffer.exceptions.EndOfTransactionException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionSealedException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.UnexpectedTxnStatusException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

public class TransactionMetaImpl implements TransactionMeta {

    private final TxnID txnID;
    private SortedMap<Long, Position> entries;
    private TxnStatus txnStatus;
    private long committedAtLedgerId = -1L;
    private long committedAtEntryId = -1L;
    private Map<String, Set<Position>> pendingAck;

    TransactionMetaImpl(TxnID txnID) {
        this.txnID = txnID;
        this.entries = new TreeMap<>();
        this.txnStatus = TxnStatus.OPEN;
        this.pendingAck = new ConcurrentHashMap<>();
    }

    @Override
    public TxnID id() {
        return this.txnID;
    }

    @Override
    public synchronized TxnStatus status() {
        return this.txnStatus;
    }

    @Override
    public int numEntries() {
        synchronized (entries) {
            return entries.size();
        }
    }

    @VisibleForTesting
    public SortedMap<Long, Position> getEntries() {
        return entries;
    }

    @Override
    public long committedAtLedgerId() {
        return committedAtLedgerId;
    }

    @Override
    public long committedAtEntryId() {
        return committedAtEntryId;
    }

    @Override
    public long lastSequenceId() {
        return entries.lastKey();
    }

    @Override
    public CompletableFuture<SortedMap<Long, Position>> readEntries(String subName, int num, long startSequenceId) {
        CompletableFuture<SortedMap<Long, Position>> readFuture = new CompletableFuture<>();

        SortedMap<Long, Position> result = new TreeMap<>();

        SortedMap<Long, Position> readEntries = entries;
        if (startSequenceId != PersistentTransactionBufferReader.DEFAULT_START_SEQUENCE_ID) {
            readEntries = entries.tailMap(startSequenceId + 1);
        }

        if (readEntries.isEmpty()) {
            readFuture.completeExceptionally(
                new EndOfTransactionException("No more entries found in transaction `" + txnID + "`"));
            return readFuture;
        }

        Set<Position> pendingPositionSet = null;
        if (subName != null) {
            pendingPositionSet = pendingAck.computeIfAbsent(subName, key -> new HashSet<>(entries.size()));
        }
        for (Map.Entry<Long, Position> longPositionEntry : readEntries.entrySet()) {
            result.put(longPositionEntry.getKey(), longPositionEntry.getValue());
            if (pendingPositionSet != null) {
                pendingPositionSet.add(longPositionEntry.getValue());
            }
        }

        readFuture.complete(result);

        return readFuture;
    }

    @Override
    public CompletableFuture<Position> appendEntry(long sequenceId, Position position) {
        CompletableFuture<Position> appendFuture = new CompletableFuture<>();
        synchronized (this) {
            if (TxnStatus.OPEN != txnStatus) {
                appendFuture.completeExceptionally(
                    new TransactionSealedException("Transaction `" + txnID + "` is " + "already sealed"));
                return appendFuture;
            }
        }
        synchronized (this.entries) {
            this.entries.put(sequenceId, position);
        }
        return CompletableFuture.completedFuture(position);
    }

    @Override
    public CompletableFuture<TransactionMeta> committingTxn() {
        CompletableFuture<TransactionMeta> committingFuture = new CompletableFuture<>();
        if (!checkStatus(TxnStatus.OPEN, committingFuture)) {
            return committingFuture;
        }
        this.txnStatus = TxnStatus.COMMITTING;
        committingFuture.complete(this);
        return committingFuture;
    }

    @Override
    public synchronized CompletableFuture<TransactionMeta> commitTxn(long committedAtLedgerId,
                                                                     long committedAtEntryId) {
        CompletableFuture<TransactionMeta> commitFuture = new CompletableFuture<>();
        if (!checkStatus(TxnStatus.COMMITTING, commitFuture)) {
            return commitFuture;
        }

        this.committedAtLedgerId = committedAtLedgerId;
        this.committedAtEntryId = committedAtEntryId;
        this.txnStatus = TxnStatus.COMMITTED;
        TransactionMeta meta = this;
        commitFuture.complete(meta);
        return commitFuture;
    }

    @Override
    public synchronized CompletableFuture<TransactionMeta> abortTxn() {
        CompletableFuture<TransactionMeta> abortFuture = new CompletableFuture<>();
        if (!checkStatus(TxnStatus.OPEN, abortFuture)) {
            return abortFuture;
        }

        this.txnStatus = TxnStatus.ABORTED;
        abortFuture.complete(this);

        return abortFuture;
    }

    private boolean checkStatus(TxnStatus expectedStatus, CompletableFuture<TransactionMeta> future) {
        if (!txnStatus.equals(expectedStatus)) {
            future.completeExceptionally(new UnexpectedTxnStatusException(txnID, expectedStatus, txnStatus));
            return false;
        }
        return true;
    }

    @Override
    public CompletableFuture<Boolean> acknowledge(String subName, List<Position> positionsAcked) {
        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        if (!pendingAck.containsKey(subName)) {
            completableFuture.completeExceptionally(new Exception("The subscription [" + subName + "] is not exist."));
            return completableFuture;
        }
        Set<Position> pendingPositionSet = pendingAck.get(subName);
        for (Position position : positionsAcked) {
            pendingPositionSet.remove(position);
        }
        completableFuture.complete(pendingPositionSet.size() == 0);
        return completableFuture;
    }
}
