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
package org.apache.pulsar.broker.transaction.timeout;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;

/**
 * An timer-task implementation of {@link TransactionTimeoutTracker}.
 */
@Slf4j
public class TransactionTimeoutTrackerImpl implements TransactionTimeoutTracker, TimerTask {

    private final Timer timer;
    private final TripleLongPriorityQueue priorityQueue = new TripleLongPriorityQueue();
    private final long tickTimeMillis;
    private final Clock clock;
    private final static long BASE_OF_MILLIS_TO_SECOND = 1000L;
    private Timeout currentTimeout;
    private final static long INITIAL_TIMEOUT = 1L;
    private long nowTaskTimeoutTime = INITIAL_TIMEOUT;
    private final long tcId;
    private final TransactionMetadataStoreService transactionMetadataStoreService;

    TransactionTimeoutTrackerImpl(long tcId, Timer timer, long tickTimeMillis,
                                  TransactionMetadataStoreService transactionMetadataStoreService) {
        this.tcId = tcId;
        this.transactionMetadataStoreService = transactionMetadataStoreService;
        this.timer = timer;
        this.tickTimeMillis  = tickTimeMillis;
        this.clock = Clock.systemUTC();
    }

    @Override
    public CompletableFuture<Boolean> addTransaction(long sequenceId, long timeout) {
        if (timeout < tickTimeMillis) {
            this.transactionMetadataStoreService.endTransaction(new TxnID(priorityQueue.peekN2(),
                    priorityQueue.peekN3()), TxnAction.ABORT_VALUE);
            return CompletableFuture.completedFuture(false);
        }
        synchronized (this){
            long nowTime = clock.millis() / BASE_OF_MILLIS_TO_SECOND;
            priorityQueue.add(timeout + nowTime, tcId, sequenceId);
            long nowTransactionTimeoutTime = nowTime + timeout;
            if (nowTaskTimeoutTime == INITIAL_TIMEOUT) {
                currentTimeout = timer.newTimeout(this, timeout, TimeUnit.SECONDS);
                nowTaskTimeoutTime = nowTransactionTimeoutTime;
            } else if (nowTaskTimeoutTime > nowTransactionTimeoutTime) {
                currentTimeout.cancel();
                currentTimeout = timer.newTimeout(this, timeout, TimeUnit.SECONDS);
                nowTaskTimeoutTime = nowTransactionTimeoutTime;
            }
        }
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public void replayAddTransaction(long sequenceId, long timeout) {
        long nowTime = clock.millis() / BASE_OF_MILLIS_TO_SECOND;
        priorityQueue.add(timeout + nowTime, tcId, sequenceId);
    }

    @Override
    public void start() {
        run(null);
    }

    @Override
    public void close() {
        priorityQueue.close();
        this.close();
    }

    @Override
    public void run(Timeout timeout) {
        synchronized (this){
            while (!priorityQueue.isEmpty()){
                long timeoutTime = priorityQueue.peekN1();
                long nowTime = clock.millis() / BASE_OF_MILLIS_TO_SECOND;
                if (timeoutTime < nowTime){
                    transactionMetadataStoreService.endTransaction(new TxnID(priorityQueue.peekN2(),
                            priorityQueue.peekN3()), TxnAction.ABORT_VALUE);
                } else {
                    currentTimeout = timer
                            .newTimeout(this,
                                    timeoutTime - clock.millis() / BASE_OF_MILLIS_TO_SECOND, TimeUnit.SECONDS);
                    nowTaskTimeoutTime = nowTime + timeoutTime;
                    break;
                }
                priorityQueue.pop();
            }
        }
    }
}
