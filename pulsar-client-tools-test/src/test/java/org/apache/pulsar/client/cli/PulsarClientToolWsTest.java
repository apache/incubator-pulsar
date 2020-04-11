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
package org.apache.pulsar.client.cli;

import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.*;


public class PulsarClientToolWsTest extends ProducerConsumerBase {
    private ProxyServer proxyServer;
    private WebSocketService service;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(Optional.of(0));
        config.setClusterName("test");
        config.setConfigurationStoreServers("dummy-zk-servers");
        service = spy(new WebSocketService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.resetConfig();
        super.internalCleanup();
        service.close();
        proxyServer.stop();
    }

    @Test(timeOut = 20000)
    public void testWebSocketUnsubscribe() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("serviceUrl", "ws://localhost:" + proxyServer.getListenPortHTTP().get());
        properties.setProperty("useTls", "false");

        String tenantName = UUID.randomUUID().toString();
        admin.tenants().createTenant(tenantName, createDefaultTenantInfo());
        String topicName = String.format("persistent://%s/ns/topic-scale-ns-0/topic", tenantName);

        int numberOfMessages = 10;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                PulsarClientTool pulsarClientToolConsumer = new PulsarClientTool(properties);
                String[] args = {"consume", "-t", "Exclusive", "-s", "sub-name", "-n",
                        Integer.toString(numberOfMessages), "--hex", "--unsubscribe", "-r", "30", topicName};
                Assert.assertEquals(pulsarClientToolConsumer.run(args), 0);
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });

        // Make sure subscription has been created
        while (true) {
            try {
                List<String> subscriptions = admin.topics().getSubscriptions(topicName);
                if (subscriptions.size() == 1) {
                    break;
                }
            } catch (Exception e) {
            }
            Thread.sleep(200);
        }

        PulsarClientTool pulsarClientToolProducer = new PulsarClientTool(properties);

        String[] args = {"produce", "--messages", "Have a nice day", "-n", Integer.toString(numberOfMessages), "-r",
                "20", "-p", "key1=value1", "-p", "key2=value2", "-k", "partition_key", topicName};
        Assert.assertEquals(pulsarClientToolProducer.run(args), 0);

        future.get();
        executor.shutdown();

        while (true) {
            try {
                List<String> subscriptions = admin.topics().getSubscriptions(topicName);
                if (subscriptions.size() == 0) {
                    break;
                }
            } catch (Exception e) {
            }
            Thread.sleep(200);
        }
    }
}
