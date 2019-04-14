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
package org.apache.bookkeeper.mledger.impl;

import com.google.common.base.Predicate;

import io.netty.buffer.ByteBuf;

import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.PositionBound;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

class OpFindNewest implements ReadEntryCallback {
    private static final Logger log = LoggerFactory.getLogger(ReadEntryCallback.class);

    private final ManagedCursorImpl cursor;
    private final PositionImpl startPosition;
    private final FindEntryCallback callback;
    private final Predicate<Entry> condition;
    private final Object ctx;

    enum State {
        checkFirst, checkLast, searching
    }

    PositionImpl searchPosition;
    boolean usePosition;
    long min;
    long max;
    Position lastMatchedPosition = null;
    State state;

    public OpFindNewest(ManagedCursorImpl cursor, PositionImpl startPosition, Predicate<Entry> condition,
            long numberOfEntries, FindEntryCallback callback, Object ctx, boolean usePosition) {
        this.cursor = cursor;
        this.startPosition = startPosition;
        this.callback = callback;
        this.condition = condition;
        this.ctx = ctx;

        this.min = 0;
        this.max = numberOfEntries;

        this.searchPosition = startPosition;
        this.state = State.checkFirst;
        log.info("Use Position was set to: " + usePosition);
        this.usePosition = usePosition;
    }

    @Override
    public void readEntryComplete(Entry entry, Object ctx) {
        final Position position = entry.getPosition();
        switch (state) {
        case checkFirst:
            if (!condition.apply(entry)) {
                if (!usePosition) {
                    callback.findEntryData((ByteBuf) null, OpFindNewest.this.ctx);
                    return;
                }
                callback.findEntryComplete((Position) null, OpFindNewest.this.ctx);
                return;
            } else {
                lastMatchedPosition = position;

                // check last entry
                state = State.checkLast;
                searchPosition = cursor.ledger.getPositionAfterN(searchPosition, max, PositionBound.startExcluded);
                find();
            }
            break;
        case checkLast:
            if (condition.apply(entry)) {
                if (!usePosition) {
                    callback.findEntryData(entry.getDataBuffer(), OpFindNewest.this.ctx);
                    return;
                }
                callback.findEntryComplete(position, OpFindNewest.this.ctx);
                return;
            } else {
                // start binary search
                state = State.searching;
                searchPosition = cursor.ledger.getPositionAfterN(startPosition, mid(), PositionBound.startExcluded);
                find();
            }
            break;
        case searching:
            if (condition.apply(entry)) {
                // mid - last
                lastMatchedPosition = position;
                min = mid();
            } else {
                // start - mid
                max = mid() - 1;
            }

            if (max <= min) {
                if (!usePosition) {
                    callback.findEntryData(entry.getDataBuffer(), OpFindNewest.this.ctx);
                }
                callback.findEntryComplete(lastMatchedPosition, OpFindNewest.this.ctx);
                return;
            }
            searchPosition = cursor.ledger.getPositionAfterN(startPosition, mid(), PositionBound.startExcluded);
            find();
        }
    }

    @Override
    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
        callback.findEntryFailed(exception, OpFindNewest.this.ctx);
    }

    public void find() {
        if (cursor.hasMoreEntries(searchPosition)) {
            cursor.ledger.asyncReadEntry(searchPosition, this, null);
        } else {
            callback.findEntryComplete(lastMatchedPosition, OpFindNewest.this.ctx);
        }
    }

    private long mid() {
        return min + Math.max((max - min) / 2, 1);
    }
}
