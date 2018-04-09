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

import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OffloadPrefixTest extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(OffloadPrefixTest.class);

    @Test
    public void testNullOffloader() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        Position p = ledger.getLastConfirmedEntry();

        for (; i < 45; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 5);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 0);
        try {
            ledger.offloadPrefix(p);
            Assert.fail("Should have thrown an exception");
        } catch (ManagedLedgerException e) {
            Assert.assertEquals(e.getCause().getClass(), CompletionException.class);
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 5);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 0);

        // add more entries to ensure we can update the ledger list
        for (; i < 55; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 6);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 0);
    }

    @Test
    public void testOffload() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
    }

    @Test
    public void testPositionOutOfRange() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        try {
            ledger.offloadPrefix(PositionImpl.earliest);
            Assert.fail("Should have thrown an exception");
        } catch (ManagedLedgerException.InvalidCursorPositionException e) {
            // expected
        }
        try {
            ledger.offloadPrefix(PositionImpl.latest);
            Assert.fail("Should have thrown an exception");
        } catch (ManagedLedgerException.InvalidCursorPositionException e) {
            // expected
        }

        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 0);
        Assert.assertEquals(offloader.offloadedLedgers().size(), 0);
    }

    @Test
    public void testPositionOnEdgeOfLedger() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 20; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);

        Position p = ledger.getLastConfirmedEntry(); // position at end of second ledger

        ledger.addEntry("entry-blah".getBytes());
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        PositionImpl firstUnoffloaded = (PositionImpl)ledger.offloadPrefix(p);

        // only the first ledger should have been offloaded
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        Assert.assertEquals(offloader.offloadedLedgers().size(), 1);
        Assert.assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(0).hasOffloadContext());
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 1);
        Assert.assertEquals(firstUnoffloaded.getLedgerId(), ledger.getLedgersInfoAsList().get(1).getLedgerId());
        Assert.assertEquals(firstUnoffloaded.getEntryId(), 0);

        // offload again, with the position in the third ledger
        PositionImpl firstUnoffloaded2 = (PositionImpl)ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        Assert.assertEquals(offloader.offloadedLedgers().size(), 2);
        Assert.assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
        Assert.assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(1).getLedgerId()));
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(0).hasOffloadContext());
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(1).hasOffloadContext());
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 2);
        Assert.assertEquals(firstUnoffloaded2.getLedgerId(), ledger.getLedgersInfoAsList().get(2).getLedgerId());
    }

    @Test
    public void testTrimOccursDuringOffload() throws Exception {
        CountDownLatch offloadStarted = new CountDownLatch(1);
        CompletableFuture<Void> blocker = new CompletableFuture<>();
        MockLedgerOffloader offloader = new MockLedgerOffloader() {
                @Override
                public CompletableFuture<byte[]> offload(ReadHandle ledger,
                                                         Map<String, String> extraMetadata) {
                    offloadStarted.countDown();
                    return blocker.thenCompose((f) -> super.offload(ledger, extraMetadata));
                }
            };

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(0, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("foobar");

        // Create 3 ledgers, saving position at start of each
        for (int i = 0; i < 21; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        PositionImpl startOfSecondLedger = PositionImpl.get(ledger.getLedgersInfoAsList().get(1).getLedgerId(), 0);
        PositionImpl startOfThirdLedger = PositionImpl.get(ledger.getLedgersInfoAsList().get(2).getLedgerId(), 0);

        // trigger an offload which should offload the first two ledgers
        OffloadCallbackPromise cbPromise = new OffloadCallbackPromise();
        ledger.asyncOffloadPrefix(startOfThirdLedger, cbPromise, null);
        offloadStarted.await();

        // trim first ledger
        cursor.markDelete(startOfSecondLedger, new HashMap<>());
        assertEventuallyTrue(() -> ledger.getLedgersInfoAsList().size() == 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 0);

        // complete offloading
        blocker.complete(null);
        cbPromise.get();

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 1);
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(0).hasOffloadContext());
        Assert.assertEquals(offloader.offloadedLedgers().size(), 1);
        Assert.assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
    }

    @Test
    public void testTrimOccursDuringOffloadLedgerDeletedBeforeOffload() throws Exception {
        CountDownLatch offloadStarted = new CountDownLatch(1);
        CompletableFuture<Long> blocker = new CompletableFuture<>();
        MockLedgerOffloader offloader = new MockLedgerOffloader() {
                @Override
                public CompletableFuture<byte[]> offload(ReadHandle ledger,
                                                         Map<String, String> extraMetadata) {
                    offloadStarted.countDown();
                    return blocker.thenCompose(
                            (trimmedLedger) -> {
                                if (trimmedLedger == ledger.getId()) {
                                    CompletableFuture<byte[]> future = new CompletableFuture<>();
                                    future.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                                    return future;
                                } else {
                                    return super.offload(ledger, extraMetadata);
                                }
                            });
                }
            };

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(0, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("foobar");

        for (int i = 0; i < 21; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        PositionImpl startOfSecondLedger = PositionImpl.get(ledger.getLedgersInfoAsList().get(1).getLedgerId(), 0);
        PositionImpl startOfThirdLedger = PositionImpl.get(ledger.getLedgersInfoAsList().get(2).getLedgerId(), 0);

        // trigger an offload which should offload the first two ledgers
        OffloadCallbackPromise cbPromise = new OffloadCallbackPromise();
        ledger.asyncOffloadPrefix(startOfThirdLedger, cbPromise, null);
        offloadStarted.await();

        // trim first ledger
        long trimmedLedger = ledger.getLedgersInfoAsList().get(0).getLedgerId();
        cursor.markDelete(startOfSecondLedger, new HashMap<>());
        assertEventuallyTrue(() -> ledger.getLedgersInfoAsList().size() == 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getLedgerId() == trimmedLedger).count(), 0);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 0);

        // complete offloading
        blocker.complete(trimmedLedger);
        cbPromise.get();

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 1);
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(0).hasOffloadContext());
        Assert.assertEquals(offloader.offloadedLedgers().size(), 1);
        Assert.assertTrue(offloader.offloadedLedgers().contains(ledger.getLedgersInfoAsList().get(0).getLedgerId()));
    }

    @Test
    public void testOffloadClosedManagedLedger() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        for (int i = 0; i < 21; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        Position p = ledger.getLastConfirmedEntry();
        ledger.close();

        try {
            ledger.offloadPrefix(p);
            Assert.fail("Should fail because ML is closed");
        } catch (ManagedLedgerException.ManagedLedgerAlreadyClosedException e) {
            // expected
        }

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 0);
        Assert.assertEquals(offloader.offloadedLedgers().size(), 0);
    }

    @Test
    public void testOffloadSamePositionTwice() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());

    }

    public void offloadThreeOneFails(int failIndex) throws Exception {
        CompletableFuture<Set<Long>> promise = new CompletableFuture<>();
        MockLedgerOffloader offloader = new ErroringMockLedgerOffloader(promise);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        int i = 0;
        for (; i < 35; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 4);

        // mark ledgers to fail
        promise.complete(ImmutableSet.of(ledger.getLedgersInfoAsList().get(failIndex).getLedgerId()));

        try {
            ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        } catch (ManagedLedgerException e) {
            Assert.assertEquals(e.getCause().getClass(), CompletionException.class);
        }

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 4);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.hasOffloadContext()).count(), 2);
        Assert.assertFalse(ledger.getLedgersInfoAsList().get(failIndex).hasOffloadContext());
    }

    @Test
    public void testOffloadThreeFirstFails() throws Exception {
        offloadThreeOneFails(0);
    }

    @Test
    public void testOffloadThreeSecondFails() throws Exception {
        offloadThreeOneFails(1);
    }

    @Test
    public void testOffloadThreeThirdFails() throws Exception {
        offloadThreeOneFails(2);
    }

    @Test
    public void testOffloadNewML() throws Exception {
        MockLedgerOffloader offloader = new MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);

        try {
            ledger.offloadPrefix(ledger.getLastConfirmedEntry());
        } catch (ManagedLedgerException.InvalidCursorPositionException e) {
            // expected
        }
        // add one entry and try again
        ledger.addEntry("foobar".getBytes());

        Position p = ledger.getLastConfirmedEntry();
        Assert.assertEquals(p, ledger.offloadPrefix(ledger.getLastConfirmedEntry()));
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        Assert.assertEquals(offloader.offloadedLedgers().size(), 0);
    }

    void assertEventuallyTrue(BooleanSupplier predicate) throws Exception {
        // wait up to 3 seconds
        for (int i = 0; i < 30 && !predicate.getAsBoolean(); i++) {
            Thread.sleep(100);
        }
        Assert.assertTrue(predicate.getAsBoolean());
    }

    static class OffloadCallbackPromise extends CompletableFuture<Position> implements OffloadCallback {
        @Override
        public void offloadComplete(Position pos, Object ctx) {
            complete(pos);
        }

        @Override
        public void offloadFailed(ManagedLedgerException exception, Object ctx) {
            completeExceptionally(exception);
        }
    }

    static class MockLedgerOffloader implements LedgerOffloader {
        ConcurrentHashMap<Long, UUID> offloads = new ConcurrentHashMap<Long, UUID>();

        Set<Long> offloadedLedgers() {
            return offloads.keySet();
        }

        @Override
        public CompletableFuture<byte[]> offload(ReadHandle ledger,
                                                 Map<String, String> extraMetadata) {
            CompletableFuture<byte[]> promise = new CompletableFuture<>();
            UUID uuid = UUID.randomUUID();
            if (offloads.putIfAbsent(ledger.getId(), uuid) == null) {
                promise.complete(uuid.toString().getBytes());
            } else {
                promise.completeExceptionally(new Exception("Already exists exception"));
            }
            return promise;
        }

        @Override
        public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, byte[] offloadContext) {
            CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
            promise.completeExceptionally(new UnsupportedOperationException());
            return promise;
        }

        @Override
        public CompletableFuture<Void> deleteOffloaded(long ledgerId, byte[] offloadContext) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            UUID uuid = UUID.fromString(new String(offloadContext));
            if (offloads.remove(ledgerId, uuid)) {
                promise.complete(null);
            } else {
                promise.completeExceptionally(new Exception("Not found"));
            }
            return promise;
        };
    }

    static class ErroringMockLedgerOffloader extends MockLedgerOffloader {
        CompletableFuture<Set<Long>> errorLedgers = new CompletableFuture<>();

        ErroringMockLedgerOffloader(CompletableFuture<Set<Long>> errorLedgers) {
            this.errorLedgers = errorLedgers;
        }

        @Override
        public CompletableFuture<byte[]> offload(ReadHandle ledger,
                                                 Map<String, String> extraMetadata) {
            return errorLedgers.thenCompose(
                            (errors) -> {
                                if (errors.contains(ledger.getId())) {
                                    CompletableFuture<byte[]> future = new CompletableFuture<>();
                                    future.completeExceptionally(new Exception("Some kind of error"));
                                    return future;
                                } else {
                                    return super.offload(ledger, extraMetadata);
                                }
                            });
        }
    }
}
