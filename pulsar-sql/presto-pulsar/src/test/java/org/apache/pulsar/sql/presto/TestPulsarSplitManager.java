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
package org.apache.pulsar.sql.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import io.airlift.log.Logger;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestPulsarSplitManager extends TestPulsarConnector {

    private static final Logger log = Logger.get(TestPulsarSplitManager.class);

    public class ResultCaptor<T> implements Answer {
        private T result = null;
        public T getResult() {
            return result;
        }

        @Override
        public T answer(InvocationOnMock invocationOnMock) throws Throwable {
            result = (T) invocationOnMock.callRealMethod();
            return result;
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter")
    public void testTopic(String delimiter) throws Exception {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        List<TopicName> topics = new LinkedList<>();
        topics.addAll(topicNames.stream().filter(topicName -> !topicName.equals(NON_SCHEMA_TOPIC)).collect(Collectors.toList()));
        for (TopicName topicName : topics) {
            setup();
            log.info("!----- topic: %s -----!", topicName);
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                    topicName.getNamespace(),
                    topicName.getLocalName(),
                    topicName.getLocalName());
            PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, TupleDomain.all());

            final ResultCaptor<Collection<PulsarSplit>> resultCaptor = new ResultCaptor<>();
            doAnswer(resultCaptor).when(this.pulsarSplitManager).getSplitsNonPartitionedTopic(anyInt(), any(), any(), any(), any());


            ConnectorSplitSource connectorSplitSource = this.pulsarSplitManager.getSplits(
                    mock(ConnectorTransactionHandle.class), mock(ConnectorSession.class),
                    pulsarTableLayoutHandle, null);

            verify(this.pulsarSplitManager, times(1))
                    .getSplitsNonPartitionedTopic(anyInt(), any(), any(), any(), any());

            int totalSize = 0;
            for (PulsarSplit pulsarSplit : resultCaptor.getResult()) {
                assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
                assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
                assertEquals(pulsarSplit.getTableName(), topicName.getLocalName());
                assertEquals(pulsarSplit.getSchema(),
                        new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema()));
                assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
                assertEquals(pulsarSplit.getStartPositionEntryId(), totalSize);
                assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
                assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, totalSize));
                assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
                assertEquals(pulsarSplit.getEndPositionEntryId(), totalSize + pulsarSplit.getSplitSize());
                assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, totalSize + pulsarSplit.getSplitSize()));

                totalSize += pulsarSplit.getSplitSize();
            }

            assertEquals(totalSize, topicsToNumEntries.get(topicName.getSchemaName()).intValue());
            cleanup();
        }

    }

    @Test(dataProvider = "rewriteNamespaceDelimiter")
    public void testPartitionedTopic(String delimiter) throws Exception {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        for (TopicName topicName : partitionedTopicNames) {
            setup();
            log.info("!----- topic: %s -----!", topicName);
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                    topicName.getNamespace(),
                    topicName.getLocalName(),
                    topicName.getLocalName());
            PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, TupleDomain.all());

            final ResultCaptor<Collection<PulsarSplit>> resultCaptor = new ResultCaptor<>();
            doAnswer(resultCaptor).when(this.pulsarSplitManager).getSplitsPartitionedTopic(anyInt(), any(), any(), any(), any());

            this.pulsarSplitManager.getSplits(mock(ConnectorTransactionHandle.class), mock(ConnectorSession.class),
                    pulsarTableLayoutHandle, null);

            verify(this.pulsarSplitManager, times(1))
                    .getSplitsPartitionedTopic(anyInt(), any(), any(), any(), any());

            int partitions = partitionedTopicsToPartitions.get(topicName.toString());

            for (int i = 0; i < partitions; i++) {
                List<PulsarSplit> splits = getSplitsForPartition(topicName.getPartition(i), resultCaptor.getResult());
                int totalSize = 0;
                for (PulsarSplit pulsarSplit : splits) {
                    assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
                    assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
                    assertEquals(pulsarSplit.getTableName(), topicName.getPartition(i).getLocalName());
                    assertEquals(pulsarSplit.getSchema(),
                            new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema()));
                    assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
                    assertEquals(pulsarSplit.getStartPositionEntryId(), totalSize);
                    assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
                    assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, totalSize));
                    assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
                    assertEquals(pulsarSplit.getEndPositionEntryId(), totalSize + pulsarSplit.getSplitSize());
                    assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, totalSize + pulsarSplit.getSplitSize()));

                    totalSize += pulsarSplit.getSplitSize();
                }

                assertEquals(totalSize, topicsToNumEntries.get(topicName.getSchemaName()).intValue());
            }

            cleanup();
        }
    }

    private List<PulsarSplit> getSplitsForPartition(TopicName target, Collection<PulsarSplit> splits) {
        return splits.stream().filter(pulsarSplit -> {
             TopicName topicName = TopicName.get(pulsarSplit.getSchemaName() + "/" + pulsarSplit.getTableName());
             return target.equals(topicName);
        }).collect(Collectors.toList());
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter")
    public void testPublishTimePredicatePushdown(String delimiter) throws Exception {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        TopicName topicName = TOPIC_1;

        setup();
        log.info("!----- topic: %s -----!", topicName);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                topicName.getNamespace(),
                topicName.getLocalName(),
                topicName.getLocalName());


        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        Domain domain = Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP, currentTimeMs + 1L, true,
                currentTimeMs + 50L, true)), false);
        domainMap.put(PulsarInternalColumn.PUBLISH_TIME.getColumnHandle(pulsarConnectorId.toString(), false), domain);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

        PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, tupleDomain);

        final ResultCaptor<Collection<PulsarSplit>> resultCaptor = new ResultCaptor<>();
        doAnswer(resultCaptor).when(this.pulsarSplitManager).getSplitsNonPartitionedTopic(anyInt(), any(), any(), any
                (), any());

        ConnectorSplitSource connectorSplitSource = this.pulsarSplitManager.getSplits(
                mock(ConnectorTransactionHandle.class), mock(ConnectorSession.class),
                pulsarTableLayoutHandle, null);


        verify(this.pulsarSplitManager, times(1))
                .getSplitsNonPartitionedTopic(anyInt(), any(), any(), any(), any());

        int totalSize = 0;
        int initalStart = 1;
        for (PulsarSplit pulsarSplit : resultCaptor.getResult()) {
            assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
            assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
            assertEquals(pulsarSplit.getTableName(), topicName.getLocalName());
            assertEquals(pulsarSplit.getSchema(),
                    new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema()));
            assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
            assertEquals(pulsarSplit.getStartPositionEntryId(), initalStart);
            assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
            assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, initalStart));
            assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
            assertEquals(pulsarSplit.getEndPositionEntryId(), initalStart + pulsarSplit.getSplitSize());
            assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, initalStart + pulsarSplit
                    .getSplitSize()));

            initalStart += pulsarSplit.getSplitSize();
            totalSize += pulsarSplit.getSplitSize();
        }
        assertEquals(totalSize, 49);

    }

    @Test(dataProvider = "rewriteNamespaceDelimiter")
    public void testPublishTimePredicatePushdownPartitionedTopic(String delimiter) throws Exception {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        TopicName topicName = PARTITIONED_TOPIC_1;

        setup();
        log.info("!----- topic: %s -----!", topicName);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                topicName.getNamespace(),
                topicName.getLocalName(),
                topicName.getLocalName());


        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        Domain domain = Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP, currentTimeMs + 1L, true,
                currentTimeMs + 50L, true)), false);
        domainMap.put(PulsarInternalColumn.PUBLISH_TIME.getColumnHandle(pulsarConnectorId.toString(), false), domain);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

        PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, tupleDomain);

        final ResultCaptor<Collection<PulsarSplit>> resultCaptor = new ResultCaptor<>();
        doAnswer(resultCaptor).when(this.pulsarSplitManager)
                .getSplitsPartitionedTopic(anyInt(), any(), any(), any(), any());

        ConnectorSplitSource connectorSplitSource = this.pulsarSplitManager.getSplits(
                mock(ConnectorTransactionHandle.class), mock(ConnectorSession.class),
                pulsarTableLayoutHandle, null);


        verify(this.pulsarSplitManager, times(1))
                .getSplitsPartitionedTopic(anyInt(), any(), any(), any(), any());


        int partitions = partitionedTopicsToPartitions.get(topicName.toString());
        for (int i = 0; i < partitions; i++) {
            List<PulsarSplit> splits = getSplitsForPartition(topicName.getPartition(i), resultCaptor.getResult());
            int totalSize = 0;
            int initialStart = 1;
            for (PulsarSplit pulsarSplit : splits) {
                assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
                assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
                assertEquals(pulsarSplit.getTableName(), topicName.getPartition(i).getLocalName());
                assertEquals(pulsarSplit.getSchema(),
                        new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema()));
                assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
                assertEquals(pulsarSplit.getStartPositionEntryId(), initialStart);
                assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
                assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, initialStart));
                assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
                assertEquals(pulsarSplit.getEndPositionEntryId(), initialStart + pulsarSplit.getSplitSize());
                assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, initialStart + pulsarSplit.getSplitSize()));

                initialStart += pulsarSplit.getSplitSize();
                totalSize += pulsarSplit.getSplitSize();
            }

            assertEquals(totalSize, 49);
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter")
    public void testPartitionFilter(String delimiter) throws Exception {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        for (TopicName topicName : partitionedTopicNames) {
            setup();
            log.info("!----- topic: %s -----!", topicName);
            PulsarTableHandle pulsarTableHandle = mock(PulsarTableHandle.class);
            when(pulsarTableHandle.getConnectorId()).thenReturn(pulsarConnectorId.toString());
            when(pulsarTableHandle.getSchemaName()).thenReturn(topicName.getNamespace());
            when(pulsarTableHandle.getTopicName()).thenReturn(topicName.getLocalName());
            when(pulsarTableHandle.getTableName()).thenReturn(topicName.getLocalName());

            // test single domain with equal low and high of "__partition__"
            Map<ColumnHandle, Domain> domainMap = new HashMap<>();
            Domain domain = Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 0L, true,
                0L, true)), false);
            domainMap.put(PulsarInternalColumn.PARTITION.getColumnHandle(pulsarConnectorId.toString(), false), domain);
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);
            Collection<PulsarSplit> splits = this.pulsarSplitManager.getSplitsPartitionedTopic(2, topicName, pulsarTableHandle,
                schemas.getSchemaInfo(topicName.getSchemaName()), tupleDomain);
            if (topicsToNumEntries.get(topicName.getSchemaName()) > 1) {
                Assert.assertEquals(splits.size(), 2);
            }
            for (PulsarSplit split : splits) {
                assertEquals(TopicName.getPartitionIndex(split.getTableName()), 0);
            }

            // test multiple domain with equal low and high of "__partition__"
            domainMap.clear();
            domain = Domain.create(ValueSet.ofRanges(
                Range.range(INTEGER, 0L, true, 0L, true),
                Range.range(INTEGER, 3L, true, 3L, true)),
                false);
            domainMap.put(PulsarInternalColumn.PARTITION.getColumnHandle(pulsarConnectorId.toString(), false), domain);
            tupleDomain = TupleDomain.withColumnDomains(domainMap);
            splits = this.pulsarSplitManager.getSplitsPartitionedTopic(1, topicName, pulsarTableHandle,
                schemas.getSchemaInfo(topicName.getSchemaName()), tupleDomain);
            if (topicsToNumEntries.get(topicName.getSchemaName()) > 1) {
                Assert.assertEquals(splits.size(), 2);
            }
            for (PulsarSplit split : splits) {
                assertTrue(TopicName.getPartitionIndex(split.getTableName()) == 0 || TopicName.getPartitionIndex(split.getTableName()) == 3);
            }

            // test single domain with unequal low and high of "__partition__"
            domainMap.clear();
            domain = Domain.create(ValueSet.ofRanges(
                Range.range(INTEGER, 0L, true, 2L, true)),
                false);
            domainMap.put(PulsarInternalColumn.PARTITION.getColumnHandle(pulsarConnectorId.toString(), false), domain);
            tupleDomain = TupleDomain.withColumnDomains(domainMap);
            splits = this.pulsarSplitManager.getSplitsPartitionedTopic(2, topicName, pulsarTableHandle,
                schemas.getSchemaInfo(topicName.getSchemaName()), tupleDomain);
            if (topicsToNumEntries.get(topicName.getSchemaName()) > 1) {
                Assert.assertEquals(splits.size(), 3);
            }
            for (PulsarSplit split : splits) {
                assertTrue(TopicName.getPartitionIndex(split.getTableName()) == 0
                    || TopicName.getPartitionIndex(split.getTableName()) == 1
                    || TopicName.getPartitionIndex(split.getTableName()) == 2);
            }

            // test multiple domain with unequal low and high of "__partition__"
            domainMap.clear();
            domain = Domain.create(ValueSet.ofRanges(
                Range.range(INTEGER, 0L, true, 1L, true),
                Range.range(INTEGER, 3L, true, 4L, true)),
                false);
            domainMap.put(PulsarInternalColumn.PARTITION.getColumnHandle(pulsarConnectorId.toString(), false), domain);
            tupleDomain = TupleDomain.withColumnDomains(domainMap);
            splits = this.pulsarSplitManager.getSplitsPartitionedTopic(2, topicName, pulsarTableHandle,
                schemas.getSchemaInfo(topicName.getSchemaName()), tupleDomain);
            if (topicsToNumEntries.get(topicName.getSchemaName()) > 1) {
                Assert.assertEquals(splits.size(), 4);
            }
            for (PulsarSplit split : splits) {
                assertTrue(TopicName.getPartitionIndex(split.getTableName()) == 0
                    || TopicName.getPartitionIndex(split.getTableName()) == 1
                    || TopicName.getPartitionIndex(split.getTableName()) == 3
                    || TopicName.getPartitionIndex(split.getTableName()) == 4);
            }
        }


    }

    @Test(dataProvider = "rewriteNamespaceDelimiter")
    public void testGetSplitNonSchema(String delimiter) throws Exception {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        TopicName topicName = NON_SCHEMA_TOPIC;
        setup();
        log.info("!----- topic: %s -----!", topicName);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
            topicName.getNamespace(),
            topicName.getLocalName(),
            topicName.getLocalName());

        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

        PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, tupleDomain);
        ConnectorSplitSource connectorSplitSource = this.pulsarSplitManager.getSplits(
            mock(ConnectorTransactionHandle.class), mock(ConnectorSession.class),
            pulsarTableLayoutHandle, null);
        assertNotNull(connectorSplitSource);
    }
}
