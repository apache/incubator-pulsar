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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * A source that pulls data from Apache NiFi using the NiFi Site-to-Site client. This source
 * produces NiFiDataPackets which encapsulate the content and attributes of a NiFi FlowFile.
 */
@Connector(
    name = "nifi",
    type = IOType.SOURCE,
    help = "A simple connector to move messages from Apache NiFi using the NiFi Site-to-Site client to a Pulsar topic",
    configClass = NiFiConfig.class)
@Slf4j
public class NiFiSource extends PushSource<NiFiDataPacket> {

    private long waitTimeMs;
    private volatile boolean isRunning = true;
    private NiFiConfig niFiConfig;
    private SiteToSiteClientConfig clientConfig;
    private SiteToSiteClient client;

    private Thread runnerThread;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        niFiConfig = NiFiConfig.load(config);
        Preconditions.checkNotNull(niFiConfig.getUrl(), "url property not set.");
        Preconditions.checkNotNull(niFiConfig.getPortName(), "portName property not set.");
        Preconditions.checkArgument(niFiConfig.getWaitTimeMs() > 0,
                "waitTimeMs must be a positive long.");

        waitTimeMs = niFiConfig.getWaitTimeMs();
        clientConfig = new SiteToSiteClient.Builder()
                .url(niFiConfig.getUrl())
                .portName(niFiConfig.getPortName())
                .requestBatchCount(niFiConfig.getRequestBatchCount())
                .buildConfig();
        client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();

        this.start();
    }

    @Override
    public void close() throws Exception {
        this.isRunning = false;
        if (null != client) {
            client.close();
        }

        if (null != runnerThread) {
            runnerThread.interrupt();
            runnerThread.join();
            runnerThread = null;
        }
    }

    protected void start() throws Exception {
        runnerThread = new Thread(new ReceiveRunnable());
        runnerThread.setName("Nifi Source Thread");
        runnerThread.start();
    }

    class ReceiveRunnable implements Runnable {

        public ReceiveRunnable() {

        }

        @Override
        public void run() {
            Transaction transaction = null;
            while (isRunning) {
                try {
                    transaction = client.createTransaction(TransferDirection.RECEIVE);
                } catch (IOException ioe) {
                    log.warn("Created NiFi transaction Failed", ioe);
                }

                if (null == transaction) {
                    try {
                        Thread.sleep(waitTimeMs);
                    } catch (InterruptedException ioe) {
                        log.warn("transaction could not be created, waiting and will try again " + waitTimeMs + " milliseconds.");
                    }
                    continue;
                }

                try {
                    DataPacket dataPacket = transaction.receive();
                    if (null == dataPacket) {
                        // no data available. Wait a bit and try again
                        try {
                            Thread.sleep(waitTimeMs);
                        } catch (InterruptedException ioe) {
                            log.warn("dataPacket could not be received, waiting and will try again " + waitTimeMs + " milliseconds.");
                        }
                        continue;
                    }

                    final List<NiFiDataPacket> dataPackets = Lists.newArrayList();
                    do {
                        // Read the data into a byte array and wrap it along with the attributes into a NiFiDataPacket.
                        final InputStream inStream = dataPacket.getData();
                        final byte[] data = new byte[(int) dataPacket.getSize()];
                        StreamUtils.fillBuffer(inStream, data);

                        final Map<String, String> attributes = dataPacket.getAttributes();
                        final NiFiDataPacket NiFiDataPacket = new StandardNiFiDataPacket(data, attributes);
                        dataPackets.add(NiFiDataPacket);
                        dataPacket = transaction.receive();
                    } while (dataPacket != null);

                    for (NiFiDataPacket dp : dataPackets) {
                        consume(new NiFiRecord(dp));
                    }

                    // Confirm transaction to verify the data
                    transaction.confirm();
                    transaction.complete();
                } catch (final IOException e) {
                    transaction.error();
                    log.warn("Failed to receive data from NiFi", e);
                }
            }
        }
    }

    static private class NiFiRecord implements Record<NiFiDataPacket>{
        private final NiFiDataPacket value;
        @Getter
        private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        public NiFiRecord(NiFiDataPacket value) {
            this.value = value;
        }

        @Override
        public NiFiDataPacket getValue() {
            return value;
        }

        @Override
        public void ack() {
            completableFuture.complete(null);
        }
    }

}
