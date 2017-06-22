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
package com.yahoo.pulsar.broker.service.persistent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.util.Rate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.service.BrokerService;
import com.yahoo.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.impl.Backoff;
import com.yahoo.pulsar.client.impl.MessageImpl;
import com.yahoo.pulsar.client.impl.ProducerImpl;
import com.yahoo.pulsar.client.impl.PulsarClientImpl;
import com.yahoo.pulsar.client.impl.SendCallback;
import com.yahoo.pulsar.common.policies.data.ReplicatorStats;
import com.yahoo.pulsar.common.util.Codec;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

public class PersistentReplicator implements ReadEntriesCallback, DeleteCallback {

    private final BrokerService brokerService;
    private final PersistentTopic topic;
    private final String topicName;
    private final ManagedCursor cursor;
    private final String localCluster;
    private final String remoteCluster;
    private final PulsarClientImpl client;

    private volatile ProducerImpl producer;

    private final int producerQueueSize;
    private static final int MaxReadBatchSize = 100;
    private int readBatchSize;

    private final int producerQueueThreshold;

    private static final AtomicIntegerFieldUpdater<PersistentReplicator> PENDING_MESSAGES_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(PersistentReplicator.class, "pendingMessages");
    private volatile int pendingMessages = 0;

    private static final int FALSE = 0;
    private static final int TRUE = 1;

    private static final AtomicIntegerFieldUpdater<PersistentReplicator> HAVE_PENDING_READ_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(PersistentReplicator.class, "havePendingRead");
    private volatile int havePendingRead = FALSE;

    private final Rate msgOut = new Rate();
    private final Rate msgExpired = new Rate();

    private static final ProducerConfiguration producerConfiguration = new ProducerConfiguration().setSendTimeout(0,
            TimeUnit.SECONDS).setBlockIfQueueFull(true);

    private final Backoff backOff = new Backoff(100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES);
    private int messageTTLInSeconds = 0;

    private final Backoff readFailureBackoff = new Backoff(1, TimeUnit.SECONDS, 1, TimeUnit.MINUTES);

    private PersistentMessageExpiryMonitor expiryMonitor;
    // for connected subscriptions, message expiry will be checked if the backlog is greater than this threshold
    private static final int MINIMUM_BACKLOG_FOR_EXPIRY_CHECK = 1000;

    private final ReplicatorStats stats = new ReplicatorStats();

    public PersistentReplicator(PersistentTopic topic, ManagedCursor cursor, String localCluster, String remoteCluster,
            BrokerService brokerService) {
        this.brokerService = brokerService;
        this.topic = topic;
        this.topicName = topic.getName();
        this.cursor = cursor;
        this.localCluster = localCluster;
        this.remoteCluster = remoteCluster;
        this.client = (PulsarClientImpl) brokerService.getReplicationClient(remoteCluster);
        this.producer = null;
        this.expiryMonitor = new PersistentMessageExpiryMonitor(topicName, Codec.decode(cursor.getName()), cursor);
        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        PENDING_MESSAGES_UPDATER.set(this, 0);
        STATE_UPDATER.set(this, State.Stopped);

        producerQueueSize = brokerService.pulsar().getConfiguration().getReplicationProducerQueueSize();
        readBatchSize = Math.min(producerQueueSize, MaxReadBatchSize);
        producerQueueThreshold = (int) (producerQueueSize * 0.9);

        startProducer();
    }

    public String getRemoteCluster() {
        return remoteCluster;
    }

    enum State {
        Stopped, Starting, Started, Stopping
    }

    private static final AtomicReferenceFieldUpdater<PersistentReplicator, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PersistentReplicator.class, State.class, "state");
    private volatile State state = State.Stopped;

    // This method needs to be synchronized with disconnects else if there is a disconnect followed by startProducer
    // the end result can be disconnect.
    public synchronized void startProducer() {
        if (STATE_UPDATER.get(this) == State.Stopping) {
            long waitTimeMs = backOff.next();
            if (log.isDebugEnabled()) {
                log.debug(
                        "[{}][{} -> {}] waiting for producer to close before attempting to reconnect, retrying in {} s",
                        topicName, localCluster, remoteCluster, waitTimeMs / 1000.0);
            }
            // BackOff before retrying
            brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
            return;
        }
        State state = STATE_UPDATER.get(this);
        if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
            if (state == State.Started) {
                // Already running
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Replicator was already running", topicName, localCluster, remoteCluster);
                }
            } else {
                log.info("[{}][{} -> {}] Replicator already being started. Replicator state: {}", topicName,
                        localCluster, remoteCluster, state);
            }

            return;
        }

        log.info("[{}][{} -> {}] Starting replicator", topicName, localCluster, remoteCluster);
        client.createProducerAsync(topicName, producerConfiguration,
                getReplicatorName(topic.replicatorPrefix, localCluster)).thenAccept(producer -> {
                    // Rewind the cursor to be sure to read again all non-acked messages sent while restarting
                    cursor.rewind();

                    cursor.cancelPendingReadRequest();
                    HAVE_PENDING_READ_UPDATER.set(this, FALSE);
                    this.producer = (ProducerImpl) producer;

                    if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Started)) {
                        log.info("[{}][{} -> {}] Created replicator producer", topicName, localCluster, remoteCluster);
                        backOff.reset();
                        // activate cursor: so, entries can be cached
                        this.cursor.setActive();
                        // read entries
                        readMoreEntries();
                    } else {
                        log.info(
                                "[{}][{} -> {}] Replicator was stopped while creating the producer. Closing it. Replicator state: {}",
                                topicName, localCluster, remoteCluster, STATE_UPDATER.get(this));
                        STATE_UPDATER.set(this, State.Stopping);
                        closeProducerAsync();
                        return;
                    }
                }).exceptionally(ex -> {
                    if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopped)) {
                        long waitTimeMs = backOff.next();
                        log.warn("[{}][{} -> {}] Failed to create remote producer ({}), retrying in {} s", topicName,
                                localCluster, remoteCluster, ex.getMessage(), waitTimeMs / 1000.0);

                        // BackOff before retrying
                        brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
                    } else {
                        log.warn("[{}][{} -> {}] Failed to create remote producer. Replicator state: {}", topicName,
                                localCluster, remoteCluster, STATE_UPDATER.get(this), ex);
                    }
                    return null;
                });

    }

    private synchronized CompletableFuture<Void> closeProducerAsync() {
        if (producer == null) {
            STATE_UPDATER.set(this, State.Stopped);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = producer.closeAsync();
        future.thenRun(() -> {
            STATE_UPDATER.set(this, State.Stopped);
            this.producer = null;
            // deactivate cursor after successfully close the producer
            this.cursor.setInactive();
        }).exceptionally(ex -> {
            long waitTimeMs = backOff.next();
            log.warn(
                    "[{}][{} -> {}] Exception: '{}' occured while trying to close the producer. retrying again in {} s",
                    topicName, localCluster, remoteCluster, ex.getMessage(), waitTimeMs / 1000.0);
            // BackOff before retrying
            brokerService.executor().schedule(this::closeProducerAsync, waitTimeMs, TimeUnit.MILLISECONDS);
            return null;
        });
        return future;
    }

    private void readMoreEntries() {
        int availablePermits = producerQueueSize - PENDING_MESSAGES_UPDATER.get(this);

        if (availablePermits > 0) {
            int messagesToRead = Math.min(availablePermits, readBatchSize);
            if (!isWritable()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Throttling replication traffic because producer is not writable",
                            topicName, localCluster, remoteCluster);
                }
                // Minimize the read size if the producer is disconnected or the window is already full
                messagesToRead = 1;
            }

            // Schedule read
            if (HAVE_PENDING_READ_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Schedule read of {} messages", topicName, localCluster, remoteCluster,
                            messagesToRead);
                }
                cursor.asyncReadEntriesOrWait(messagesToRead, this, null);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Not scheduling read due to pending read. Messages To Read {}", topicName,
                            localCluster, remoteCluster, messagesToRead);
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{} -> {}] Producer queue is full, pause reading", topicName, localCluster,
                        remoteCluster);
            }
        }
    }

    @Override
    public void readEntriesComplete(List<Entry> entries, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{} -> {}] Read entries complete of {} messages", topicName, localCluster, remoteCluster,
                    entries.size());
        }

        if (readBatchSize < MaxReadBatchSize) {
            int newReadBatchSize = Math.min(readBatchSize * 2, MaxReadBatchSize);
            if (log.isDebugEnabled()) {
                log.debug("[{}][{} -> {}] Increasing read batch size from {} to {}", topicName, localCluster,
                        remoteCluster, readBatchSize, newReadBatchSize);
            }

            readBatchSize = newReadBatchSize;
        }

        readFailureBackoff.reduceToHalf();

        boolean atLeastOneMessageSentForReplication = false;

        try {
            // This flag is set to true when we skip atleast one local message,
            // in order to skip remaining local messages.
            boolean isLocalMessageSkippedOnce = false;
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                int length = entry.getLength();
                ByteBuf headersAndPayload = entry.getDataBuffer();
                MessageImpl msg;
                try {
                    msg = MessageImpl.deserialize(headersAndPayload);
                } catch (Throwable t) {
                    log.error("[{}][{} -> {}] Failed to deserialize message at {} (buffer size: {}): {}", topicName,
                            localCluster, remoteCluster, entry.getPosition(), length, t.getMessage(), t);
                    cursor.asyncDelete(entry.getPosition(), this, entry.getPosition());
                    entry.release();
                    continue;
                }

                if (msg.isReplicated()) {
                    // Discard messages that were already replicated into this region
                    cursor.asyncDelete(entry.getPosition(), this, entry.getPosition());
                    entry.release();
                    msg.recycle();
                    continue;
                }

                if (msg.hasReplicateTo() && !msg.getReplicateTo().contains(remoteCluster)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{} -> {}] Skipping message at {} / msg-id: {}: replicateTo {}", topicName,
                                localCluster, remoteCluster, entry.getPosition(), msg.getMessageId(),
                                msg.getReplicateTo());
                    }
                    cursor.asyncDelete(entry.getPosition(), this, entry.getPosition());
                    entry.release();
                    msg.recycle();
                    continue;
                }

                if (msg.isExpired(messageTTLInSeconds)) {
                    msgExpired.recordEvent(0 /* no value stat */);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{} -> {}] Discarding expired message at {} / msg-id: {}", topicName,
                                localCluster, remoteCluster, entry.getPosition(), msg.getMessageId());
                    }
                    cursor.asyncDelete(entry.getPosition(), this, entry.getPosition());
                    entry.release();
                    msg.recycle();
                    continue;
                }

                if (STATE_UPDATER.get(this) != State.Started || isLocalMessageSkippedOnce) {
                    // The producer is not ready yet after having stopped/restarted. Drop the message because it will
                    // recovered when the producer is ready
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{} -> {}] Dropping read message at {} because producer is not ready", topicName,
                                localCluster, remoteCluster, entry.getPosition());
                    }
                    isLocalMessageSkippedOnce = true;
                    entry.release();
                    msg.recycle();
                    continue;
                }

                // Increment pending messages for messages produced locally
                PENDING_MESSAGES_UPDATER.incrementAndGet(this);

                msgOut.recordEvent(headersAndPayload.readableBytes());

                msg.setReplicatedFrom(localCluster);

                headersAndPayload.retain();

                producer.sendAsync(msg, ProducerSendCallback.create(this, entry, msg));
                atLeastOneMessageSentForReplication = true;
            }
        } catch (Exception e) {
            log.error("[{}][{} -> {}] Unexpected exception: {}", topicName, localCluster, remoteCluster, e.getMessage(),
                    e);
        }

        HAVE_PENDING_READ_UPDATER.set(this, FALSE);

        if (atLeastOneMessageSentForReplication && !isWritable()) {
            // Don't read any more entries until the current pending entries are persisted
            if (log.isDebugEnabled()) {
                log.debug("[{}][{} -> {}] Pausing replication traffic. at-least-one: {} is-writable: {}", topicName,
                        localCluster, remoteCluster, atLeastOneMessageSentForReplication, isWritable());
            }
        } else {
            readMoreEntries();
        }
    }

    public void updateCursorState() {
        if (producer != null && producer.isConnected()) {
            this.cursor.setActive();
        } else {
            this.cursor.setInactive();
        }
    }

    private static final class ProducerSendCallback implements SendCallback {
        private PersistentReplicator replicator;
        private Entry entry;
        private MessageImpl msg;

        @Override
        public void sendComplete(Exception exception) {
            if (exception != null) {
                log.error("[{}][{} -> {}] Error producing on remote broker", replicator.topicName,
                        replicator.localCluster, replicator.remoteCluster, exception);
                // cursor shoud be rewinded since it was incremented when readMoreEntries
                replicator.cursor.rewind();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Message persisted on remote broker", replicator.topicName,
                            replicator.localCluster, replicator.remoteCluster);
                }
                replicator.cursor.asyncDelete(entry.getPosition(), replicator, entry.getPosition());
            }
            entry.release();

            int pending = PENDING_MESSAGES_UPDATER.decrementAndGet(replicator);

            // In general, we schedule a new batch read operation when the occupied queue size gets smaller than half
            // the max size, unless another read operation is already in progress.
            // If the producer is not currently writable (disconnected or TCP window full), we want to defer the reads
            // until we have emptied the whole queue, and at that point we will read a batch of 1 single message if the
            // producer is still not "writable".
            if (pending < replicator.producerQueueThreshold //
                    && HAVE_PENDING_READ_UPDATER.get(replicator) == FALSE //
            ) {
                if (pending == 0 || replicator.producer.isWritable()) {
                    replicator.readMoreEntries();
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{} -> {}] Not resuming reads. pending: {} is-writable: {}",
                                replicator.topicName, replicator.localCluster, replicator.remoteCluster, pending,
                                replicator.producer.isWritable());
                    }
                }
            }

            recycle();
        }

        private final Handle recyclerHandle;

        private ProducerSendCallback(Handle recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static ProducerSendCallback create(PersistentReplicator replicator, Entry entry, MessageImpl msg) {
            ProducerSendCallback sendCallback = RECYCLER.get();
            sendCallback.replicator = replicator;
            sendCallback.entry = entry;
            sendCallback.msg = msg;
            return sendCallback;
        }

        private void recycle() {
            replicator = null;
            entry = null; //already released and recycled on sendComplete
            if (msg != null) {
                msg.recycle();
                msg = null;
            }
            RECYCLER.recycle(this, recyclerHandle);
        }

        private static final Recycler<ProducerSendCallback> RECYCLER = new Recycler<ProducerSendCallback>() {
            @Override
            protected ProducerSendCallback newObject(Handle handle) {
                return new ProducerSendCallback(handle);
            }

        };

        @Override
        public void addCallback(SendCallback scb) {
            // noop
        }

        @Override
        public SendCallback getNextSendCallback() {
            return null;
        }

        @Override
        public CompletableFuture<MessageId> getFuture() {
            return null;
        }
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        if (STATE_UPDATER.get(this) != State.Started) {
            log.info("[{}][{} -> {}] Replicator was stopped while reading entries. Stop reading. Replicator state: {}",
                    topic, localCluster, remoteCluster, STATE_UPDATER.get(this));
            return;
        }

        // Reduce read batch size to avoid flooding bookies with retries
        readBatchSize = 1;

        long waitTimeMillis = readFailureBackoff.next();

        if(exception instanceof CursorAlreadyClosedException) {
            log.error("[{}][{} -> {}] Error reading entries because replicator is already deleted and cursor is already closed {}, ({})", topic, localCluster,
                    remoteCluster, ctx, exception.getMessage(), exception);
            // replicator is already deleted and cursor is already closed so, producer should also be stopped
            closeProducerAsync();
            return;
        }else if (!(exception instanceof TooManyRequestsException)) {
            log.error("[{}][{} -> {}] Error reading entries at {}. Retrying to read in {}s. ({})", topic, localCluster,
                    remoteCluster, ctx, waitTimeMillis / 1000.0, exception.getMessage(), exception);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{} -> {}] Throttled by bookies while reading at {}. Retrying to read in {}s. ({})",
                        topic, localCluster, remoteCluster, ctx, waitTimeMillis / 1000.0, exception.getMessage(),
                        exception);
            }
        }

        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        brokerService.executor().schedule(this::readMoreEntries, waitTimeMillis, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> clearBacklog() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{} -> {}] Backlog size before clearing: {}", topicName, localCluster, remoteCluster,
                    cursor.getNumberOfEntriesInBacklog());
        }

        cursor.asyncClearBacklog(new ClearBacklogCallback() {
            @Override
            public void clearBacklogComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Backlog size after clearing: {}", topicName, localCluster, remoteCluster,
                            cursor.getNumberOfEntriesInBacklog());
                }
                future.complete(null);
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}][{} -> {}] Failed to clear backlog", topicName, localCluster, remoteCluster, exception);
                future.completeExceptionally(exception);
            }
        }, null);

        return future;
    }

    public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{} -> {}] Skipping {} messages, current backlog {}", topicName, localCluster, remoteCluster,
                    numMessagesToSkip, cursor.getNumberOfEntriesInBacklog());
        }
        cursor.asyncSkipEntries(numMessagesToSkip, IndividualDeletedEntries.Exclude,
                new AsyncCallbacks.SkipEntriesCallback() {
                    @Override
                    public void skipEntriesComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{} -> {}] Skipped {} messages, new backlog {}", topicName, localCluster,
                                    remoteCluster, numMessagesToSkip, cursor.getNumberOfEntriesInBacklog());
                        }
                        future.complete(null);
                    }

                    @Override
                    public void skipEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{} -> {}] Failed to skip {} messages", topicName, localCluster, remoteCluster,
                                numMessagesToSkip, exception);
                        future.completeExceptionally(exception);
                    }
                }, null);

        return future;
    }

    public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("[{}][{} -> {}] Getting message at position {}", topicName, localCluster, remoteCluster,
                    messagePosition);
        }

        cursor.asyncGetNthEntry(messagePosition, IndividualDeletedEntries.Exclude, new ReadEntryCallback() {

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                future.complete(entry);
            }
        }, null);

        return future;
    }

    @Override
    public void deleteComplete(Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{} -> {}] Deleted message at {}", topicName, localCluster, remoteCluster, ctx);
        }
    }

    @Override
    public void deleteFailed(ManagedLedgerException exception, Object ctx) {
        log.error("[{}][{} -> {}] Failed to delete message at {}: {}", topicName, localCluster, remoteCluster, ctx,
                exception.getMessage(), exception);
    }

    public CompletableFuture<Void> disconnect() {
        return disconnect(false);
    }

    public synchronized CompletableFuture<Void> disconnect(boolean failIfHasBacklog) {
        if (failIfHasBacklog && cursor.getNumberOfEntriesInBacklog() > 0) {
            CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();
            disconnectFuture.completeExceptionally(new TopicBusyException("Cannot close a replicator with backlog"));
            log.debug("[{}][{} -> {}] Replicator disconnect failed since topic has backlog", topicName, localCluster,
                    remoteCluster);
            return disconnectFuture;
        }

        if (STATE_UPDATER.get(this) == State.Stopping) {
            // Do nothing since the all "STATE_UPDATER.set(this, Stopping)" instructions are followed by closeProducerAsync()
            // which will at some point change the state to stopped
            return CompletableFuture.completedFuture(null);
        }
        
        if (producer != null && (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopping)
                || STATE_UPDATER.compareAndSet(this, State.Started, State.Stopping))) {
            log.info("[{}][{} -> {}] Disconnect replicator at position {} with backlog {}", topicName, localCluster,
                    remoteCluster, cursor.getMarkDeletedPosition(), cursor.getNumberOfEntriesInBacklog());
            return closeProducerAsync();
        }
        
        STATE_UPDATER.set(this, State.Stopped);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> remove() {
        // TODO Auto-generated method stub
        return null;
    }

    public void updateRates() {
        msgOut.calculateRate();
        msgExpired.calculateRate();
        stats.msgRateOut = msgOut.getRate();
        stats.msgThroughputOut = msgOut.getValueRate();
        stats.msgRateExpired = msgExpired.getRate() + expiryMonitor.getMessageExpiryRate();
    }

    public ReplicatorStats getStats() {
        stats.replicationBacklog = cursor.getNumberOfEntriesInBacklog();
        stats.connected = producer != null && producer.isConnected();
        stats.replicationDelayInSeconds = getReplicationDelayInSeconds();

        ProducerImpl producer = this.producer;
        if (producer != null) {
            stats.outboundConnection = producer.getConnectionId();
            stats.outboundConnectedSince = producer.getConnectedSince();
        } else {
            stats.outboundConnection = null;
            stats.outboundConnectedSince = null;
        }

        return stats;
    }

    public void updateMessageTTL(int messageTTLInSeconds) {
        this.messageTTLInSeconds = messageTTLInSeconds;
    }

    private long getReplicationDelayInSeconds() {
        if (producer != null) {
            return TimeUnit.MILLISECONDS.toSeconds(producer.getDelayInMillis());
        }
        return 0L;
    }

    public void expireMessages(int messageTTLInSeconds) {
        if ((cursor.getNumberOfEntriesInBacklog() == 0)
                || (cursor.getNumberOfEntriesInBacklog() < MINIMUM_BACKLOG_FOR_EXPIRY_CHECK
                        && !topic.isOldestMessageExpired(cursor, messageTTLInSeconds))) {
            // don't do anything for almost caught-up connected subscriptions
            return;
        }
        expiryMonitor.expireMessages(messageTTLInSeconds);
    }

    private boolean isWritable() {
        ProducerImpl producer = this.producer;
        return producer != null && producer.isWritable();
    }

    public static void setReplicatorQueueSize(int queueSize) {
        producerConfiguration.setMaxPendingMessages(queueSize);
    }

    public static String getRemoteCluster(String remoteCursor) {
        String[] split = remoteCursor.split("\\.");
        return split[split.length - 1];
    }

    public static String getReplicatorName(String replicatorPrefix, String cluster) {
        return String.format("%s.%s", replicatorPrefix, cluster);
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentReplicator.class);
}
