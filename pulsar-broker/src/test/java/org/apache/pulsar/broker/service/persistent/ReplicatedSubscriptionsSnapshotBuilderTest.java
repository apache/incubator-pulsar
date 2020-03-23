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
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ClusterMessageId;
import org.apache.pulsar.common.api.proto.PulsarMarkers.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshotRequest;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshotResponse;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReplicatedSubscriptionsSnapshotBuilderTest {

    private final String localCluster = "a";
    private long currentTime = 0;
    private Clock clock;
    private ServiceConfiguration conf;
    private PersistentSubscription sub;
    private ReplicatedSubscriptionsController controller;
    private List<ByteBuf> markers;
    private List<ByteBuf> snapshots;
    private List<ReplicatedSubscriptionsSnapshot> snapshotsProcessedByBuilder;

    @DataProvider(name = "MockedDispatchers")
    public static Object[][] mockedDispatchers() {
        Dispatcher dispatcherWithConsumerAvailable = mock(Dispatcher.class);
        when(dispatcherWithConsumerAvailable.isAtleastOneConsumerAvailable()).thenReturn(true);

        Dispatcher dispatcherWithNoConsumerAvailable = mock(Dispatcher.class);
        when(dispatcherWithNoConsumerAvailable.isAtleastOneConsumerAvailable()).thenReturn(false);

        return new Object[][] { { dispatcherWithConsumerAvailable }, { dispatcherWithNoConsumerAvailable }, { null } };
    }

    @BeforeMethod
    public void setup() {
        clock = mock(Clock.class);
        when(clock.millis()).thenAnswer(invocation -> currentTime);

        conf = new ServiceConfiguration();
        conf.setReplicatedSubscriptionsSnapshotTimeoutSeconds(3);

        markers = new ArrayList<>();
        snapshots = new ArrayList<>();
        snapshotsProcessedByBuilder = new ArrayList<>();

        sub = mock(PersistentSubscription.class);
        doAnswer(invocation -> {
            ReplicatedSubscriptionsSnapshot snapshot = invocation.getArgument(0, ReplicatedSubscriptionsSnapshot.class);
            snapshotsProcessedByBuilder.add(snapshot);
            return null;
        }).when(sub).processReplicatedSubscriptionSnapshot(any(ReplicatedSubscriptionsSnapshot.class));

        ConcurrentOpenHashMap<String, PersistentSubscription> subs = new ConcurrentOpenHashMap<>();
        subs.put("sub1", sub);

        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn("persistent://my-tenant/ms-ns/my-topic");
        when(topic.getSubscriptions()).thenReturn(subs);

        controller = mock(ReplicatedSubscriptionsController.class);
        when(controller.localCluster()).thenReturn(localCluster);
        when(controller.topic()).thenReturn(topic);
        doAnswer(invocation -> {
            ByteBuf marker = invocation.getArgument(0, ByteBuf.class);
            Commands.skipMessageMetadata(marker);
            markers.add(marker);
            return null;
        }).when(controller)
                .writeMarker(any(ByteBuf.class));
        doAnswer(invocation -> {
            ByteBuf snapshot = invocation.getArgument(0, ByteBuf.class);
            Commands.skipMessageMetadata(snapshot);
            snapshots.add(snapshot);
            return null;
        }).when(controller)
                .writeSnapshot(any(ByteBuf.class));
    }

    @Test(dataProvider = "MockedDispatchers")
    public void testBuildSnapshotWith2Clusters(Dispatcher dispatcher) throws Exception {
        when(sub.getDispatcher()).thenReturn(dispatcher);
        List<String> remoteClusters = Arrays.asList("b");

        ReplicatedSubscriptionsSnapshotBuilder builder = new ReplicatedSubscriptionsSnapshotBuilder(controller,
                remoteClusters, conf, clock);

        assertTrue(markers.isEmpty());

        builder.start();

        // Should have sent out a marker to initiate the snapshot
        assertEquals(markers.size(), 1);
        ReplicatedSubscriptionsSnapshotRequest request = Markers
                .parseReplicatedSubscriptionsSnapshotRequest(markers.remove(0));
        assertEquals(request.getSourceCluster(), localCluster);

        // Simulate the responses coming back
        builder.receivedSnapshotResponse(new PositionImpl(1, 1),
                ReplicatedSubscriptionsSnapshotResponse.newBuilder()
                        .setSnapshotId("snapshot-1")
                        .setCluster(ClusterMessageId.newBuilder()
                                .setCluster("b")
                                .setMessageId(MessageIdData.newBuilder()
                                        .setLedgerId(11)
                                        .setEntryId(11)
                                        .build()))
                        .build());

        // At this point the snapshot should be created
        assertEquals(snapshots.size(), 1);
        ReplicatedSubscriptionsSnapshot snapshot = Markers.parseReplicatedSubscriptionsSnapshot(snapshots.remove(0));
        assertEquals(snapshot.getClustersCount(), 1);
        assertEquals(snapshot.getClusters(0).getCluster(), "b");
        assertEquals(snapshot.getClusters(0).getMessageId().getLedgerId(), 11);
        assertEquals(snapshot.getClusters(0).getMessageId().getEntryId(), 11);

        assertEquals(snapshot.getLocalMessageId().getLedgerId(), 1);
        assertEquals(snapshot.getLocalMessageId().getEntryId(), 1);

        if (dispatcher != null && dispatcher.isAtleastOneConsumerAvailable()) {
            assertTrue(snapshotsProcessedByBuilder.isEmpty());
        } else {
            assertEquals(snapshotsProcessedByBuilder.size(), 1);
            ReplicatedSubscriptionsSnapshot snapshot2 = snapshotsProcessedByBuilder.remove(0);
            assertEquals(snapshot2.getClustersCount(), 1);

            assertEquals(snapshot2.getClusters(0).getCluster(), "b");
            assertEquals(snapshot2.getClusters(0).getMessageId().getLedgerId(), 11);
            assertEquals(snapshot2.getClusters(0).getMessageId().getEntryId(), 11);

            assertEquals(snapshot2.getLocalMessageId().getLedgerId(), 1);
            assertEquals(snapshot2.getLocalMessageId().getEntryId(), 1);
        }
    }

    @Test(dataProvider = "MockedDispatchers")
    public void testBuildSnapshotWith3Clusters(Dispatcher dispatcher) throws Exception {
        when(sub.getDispatcher()).thenReturn(dispatcher);
        List<String> remoteClusters = Arrays.asList("b", "c");

        ReplicatedSubscriptionsSnapshotBuilder builder = new ReplicatedSubscriptionsSnapshotBuilder(controller,
                remoteClusters, conf, clock);

        assertTrue(markers.isEmpty());

        builder.start();

        // Should have sent out a marker to initiate the snapshot
        assertEquals(markers.size(), 1);
        ReplicatedSubscriptionsSnapshotRequest request = Markers
                .parseReplicatedSubscriptionsSnapshotRequest(markers.remove(0));
        assertEquals(request.getSourceCluster(), localCluster);

        // Simulate the responses coming back
        builder.receivedSnapshotResponse(new PositionImpl(1, 1),
                ReplicatedSubscriptionsSnapshotResponse.newBuilder()
                        .setSnapshotId("snapshot-1")
                        .setCluster(ClusterMessageId.newBuilder()
                                .setCluster("b")
                                .setMessageId(MessageIdData.newBuilder()
                                        .setLedgerId(11)
                                        .setEntryId(11)
                                        .build()))
                        .build());

        // No markers should be sent out
        assertTrue(markers.isEmpty());
        assertTrue(snapshots.isEmpty());
        assertTrue(snapshotsProcessedByBuilder.isEmpty());

        builder.receivedSnapshotResponse(new PositionImpl(2, 2),
                ReplicatedSubscriptionsSnapshotResponse.newBuilder()
                        .setSnapshotId("snapshot-1")
                        .setCluster(ClusterMessageId.newBuilder()
                                .setCluster("c")
                                .setMessageId(MessageIdData.newBuilder()
                                        .setLedgerId(22)
                                        .setEntryId(22)
                                        .build()))
                        .build());

        // Since we have 2 remote clusters, a 2nd round of snapshot will be taken
        assertEquals(markers.size(), 1);
        request = Markers.parseReplicatedSubscriptionsSnapshotRequest(markers.remove(0));
        assertEquals(request.getSourceCluster(), localCluster);
        assertTrue(snapshots.isEmpty());
        assertTrue(snapshotsProcessedByBuilder.isEmpty());

        // Responses coming back
        builder.receivedSnapshotResponse(new PositionImpl(3, 3),
                ReplicatedSubscriptionsSnapshotResponse.newBuilder()
                        .setSnapshotId("snapshot-1")
                        .setCluster(ClusterMessageId.newBuilder()
                                .setCluster("b")
                                .setMessageId(MessageIdData.newBuilder()
                                        .setLedgerId(33)
                                        .setEntryId(33)
                                        .build()))
                        .build());

        // No markers should be sent out
        assertTrue(markers.isEmpty());
        assertTrue(snapshots.isEmpty());
        assertTrue(snapshotsProcessedByBuilder.isEmpty());

        builder.receivedSnapshotResponse(new PositionImpl(4, 4),
                ReplicatedSubscriptionsSnapshotResponse.newBuilder()
                        .setSnapshotId("snapshot-1")
                        .setCluster(ClusterMessageId.newBuilder()
                                .setCluster("c")
                                .setMessageId(MessageIdData.newBuilder()
                                        .setLedgerId(44)
                                        .setEntryId(44)
                                        .build()))
                        .build());

        // At this point the snapshot should be created
        assertEquals(snapshots.size(), 1);
        ReplicatedSubscriptionsSnapshot snapshot = Markers.parseReplicatedSubscriptionsSnapshot(snapshots.remove(0));
        assertEquals(snapshot.getClustersCount(), 2);
        assertEquals(snapshot.getClusters(0).getCluster(), "b");
        assertEquals(snapshot.getClusters(0).getMessageId().getLedgerId(), 11);
        assertEquals(snapshot.getClusters(0).getMessageId().getEntryId(), 11);

        assertEquals(snapshot.getClusters(1).getCluster(), "c");
        assertEquals(snapshot.getClusters(1).getMessageId().getLedgerId(), 22);
        assertEquals(snapshot.getClusters(1).getMessageId().getEntryId(), 22);

        assertEquals(snapshot.getLocalMessageId().getLedgerId(), 4);
        assertEquals(snapshot.getLocalMessageId().getEntryId(), 4);

        if (dispatcher != null && dispatcher.isAtleastOneConsumerAvailable()) {
            assertTrue(snapshotsProcessedByBuilder.isEmpty());
        } else {
            assertEquals(snapshotsProcessedByBuilder.size(), 1);
            ReplicatedSubscriptionsSnapshot snapshot2 = snapshotsProcessedByBuilder.remove(0);
            assertEquals(snapshot2.getClustersCount(), 2);

            assertEquals(snapshot2.getClusters(0).getCluster(), "b");
            assertEquals(snapshot2.getClusters(0).getMessageId().getLedgerId(), 11);
            assertEquals(snapshot2.getClusters(0).getMessageId().getEntryId(), 11);

            assertEquals(snapshot2.getClusters(1).getCluster(), "c");
            assertEquals(snapshot2.getClusters(1).getMessageId().getLedgerId(), 22);
            assertEquals(snapshot2.getClusters(1).getMessageId().getEntryId(), 22);

            assertEquals(snapshot2.getLocalMessageId().getLedgerId(), 4);
            assertEquals(snapshot2.getLocalMessageId().getEntryId(), 4);
        }
    }

    @Test
    public void testBuildTimeout() throws Exception {
        List<String> remoteClusters = Arrays.asList("b");

        ReplicatedSubscriptionsSnapshotBuilder builder = new ReplicatedSubscriptionsSnapshotBuilder(controller,
                remoteClusters, conf, clock);

        assertFalse(builder.isTimedOut());

        builder.start();

        currentTime = 2000;

        assertFalse(builder.isTimedOut());

        currentTime = 5000;

        assertTrue(builder.isTimedOut());
    }
}
