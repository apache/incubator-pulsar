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
package org.apache.pulsar.io.nifi;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * A sink that delivers data to Apache NiFi using the NiFi Site-to-Site client. The sink requires
 * a NiFiDataPacketBuilder which can create instances of NiFiDataPacket from the incoming data.
 */
@Connector(
    name = "nifi",
    type = IOType.SINK,
    help = "The NiFiSink is used for moving messages from Pulsar to Apache NiFi using the NiFi Site-to-Site client.",
    configClass = NiFiConfig.class
)
@Slf4j
public class NiFiSink implements Sink<NiFiDataPacket> {

    private NiFiConfig niFiConfig;
    private SiteToSiteClientConfig clientConfig;

    private int requestBatchCount;
    private long waitTimeMs;
    private List<Record<NiFiDataPacket>>  currentList;
    private ArrayBlockingQueue<Record<NiFiDataPacket>> recordsForPush;

    private ScheduledExecutorService recordsPushExecutor;
    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        niFiConfig = NiFiConfig.load(config);
        Preconditions.checkNotNull(niFiConfig.getUrl(), "url property not set.");
        Preconditions.checkNotNull(niFiConfig.getPortName(), "portName property not set.");
        Preconditions.checkArgument(niFiConfig.getRequestBatchCount() > 0,
                "requestBatchCount must be a positive integer.");

        clientConfig = new SiteToSiteClient.Builder()
                .url(niFiConfig.getUrl())
                .portName(niFiConfig.getPortName())
                .requestBatchCount(niFiConfig.getRequestBatchCount())
                .buildConfig();

        requestBatchCount = niFiConfig.getRequestBatchCount();
        waitTimeMs = niFiConfig.getWaitTimeMs();
        currentList= Lists.newArrayList();
        recordsForPush = new ArrayBlockingQueue<>(requestBatchCount);

        flushExecutor = Executors.newScheduledThreadPool(1);
        recordsPushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(() -> flush(), waitTimeMs, waitTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        if (null != recordsPushExecutor) {
            recordsPushExecutor.shutdown();
        }

        if (null != flushExecutor) {
            flushExecutor.shutdown();
        }

    }

    @Override
    public void write(Record<NiFiDataPacket> record) {
        int number;
        synchronized (currentList) {
            currentList.add(record);
            number = currentList.size();
        }

        if (number == requestBatchCount) {
            recordsPushExecutor.schedule(() -> recordsPush(), 0, TimeUnit.MILLISECONDS);
        }
    }

    private void recordsPush(){
        List<Record<NiFiDataPacket>> swapList = Lists.newArrayList();

        synchronized (currentList) {
            if (!currentList.isEmpty()) {
                swapList.addAll(currentList);
                currentList = Lists.newArrayList();
            }
        }

        if (CollectionUtils.isNotEmpty(swapList)) {
            for (Record<NiFiDataPacket> record : swapList) {
                try {
                    recordsForPush.put(record);
                } catch (InterruptedException e) {
                    log.warn("Record put was interrupted ", e);
                }
            }
        }
    }

    private void flush() {
        try (final SiteToSiteClient client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build()) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);
            while (!recordsForPush.isEmpty()) {
                Record<NiFiDataPacket> record = null;
                try {
                    record = recordsForPush.take();
                    NiFiDataPacket niFiDataPacket = record.getValue();
                    transaction.send(niFiDataPacket.getContent(), niFiDataPacket.getAttributes());
                    transaction.confirm();
                    transaction.complete();
                    record.ack();
                } catch (InterruptedException e) {
                    log.warn("Record flush thread was interrupted", e);
                    if (null != record) {
                        record.fail();
                    }
                }
            }
        } catch (final IOException ioe) {
            log.warn("Failed to receive data from NiFi", ioe);
        }
    }
}
