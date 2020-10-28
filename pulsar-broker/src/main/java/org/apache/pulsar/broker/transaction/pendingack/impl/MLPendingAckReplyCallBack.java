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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckReplyCallBack;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarTransaction.PendingAckMetadataEntry;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.io.core.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MLPendingAckStore reply call back.
 */
public class MLPendingAckReplyCallBack implements PendingAckReplyCallBack {

    private final MLPendingAckStore mlPendingAckStore;

    private final PendingAckHandleImpl pendingAckHandle;;

    public MLPendingAckReplyCallBack(MLPendingAckStore mlPendingAckStore, PendingAckHandleImpl pendingAckHandle) {
        this.mlPendingAckStore = mlPendingAckStore;
        this.pendingAckHandle = pendingAckHandle;
    }

    @Override
    public void replayComplete() {
        log.info("Topic name : [{}], SubName : [{}] pending ack state reply success!",
                pendingAckHandle.getTopicName(), pendingAckHandle.getSubName());
        if (pendingAckHandle.changeToReadyState()) {
            pendingAckHandle.readyFutureComplete();
            log.info("Topic name : [{}], SubName : [{}] pending ack state reply success!",
                    pendingAckHandle.getTopicName(), pendingAckHandle.getSubName());
        } else {
            log.error("Topic name : [{}], SubName : [{}] pending ack state reply fail!",
                    pendingAckHandle.getTopicName(), pendingAckHandle.getSubName());
        }
    }

    @Override
    public void handleMetadataEntry(Position position, PendingAckMetadataEntry pendingAckMetadataEntry) {
        TxnID txnID = new TxnID(pendingAckMetadataEntry.getTxnidMostBits(),
                pendingAckMetadataEntry.getTxnidLeastBits());
        PositionImpl opPosition;
        if (pendingAckMetadataEntry.getAckSetCount() == 0) {
            opPosition = PositionImpl.get(pendingAckMetadataEntry.getLedgerId(),
                    pendingAckMetadataEntry.getEntryId());
        } else {
            opPosition = PositionImpl.get(pendingAckMetadataEntry.getLedgerId(),
                    pendingAckMetadataEntry.getEntryId(),
                    SafeCollectionUtils.longListToArray(pendingAckMetadataEntry.getAckSetList()));
        }

        if (pendingAckMetadataEntry.getAckType().getNumber() == AckType.Cumulative.getNumber()) {
            this.mlPendingAckStore.pendingCumulativeAckPosition = new KeyValue<>(txnID, position);
        } else {
            if (this.mlPendingAckStore.pendingIndividualAckPersistentMap == null) {
                this.mlPendingAckStore.pendingIndividualAckPersistentMap = new ConcurrentOpenHashMap<>();
            }
            ConcurrentOpenHashSet<Position> positions =
                    this.mlPendingAckStore.pendingIndividualAckPersistentMap
                            .computeIfAbsent(txnID, t -> new ConcurrentOpenHashSet<>());
            positions.add(position);
        }
        this.pendingAckHandle.handleMetadataEntry(txnID, opPosition, AckType.valueOf(pendingAckMetadataEntry.getAckType().getNumber()));
    }

    private static final Logger log = LoggerFactory.getLogger(MLPendingAckReplyCallBack.class);
}