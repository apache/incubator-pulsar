
/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.websocket.proxy;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.client.api.ProducerConsumerBase;
import com.yahoo.pulsar.websocket.WebSocketService;
import com.yahoo.pulsar.websocket.service.ProxyServer;
import com.yahoo.pulsar.websocket.service.WebSocketProxyConfiguration;
import com.yahoo.pulsar.websocket.service.WebSocketServiceStarter;

public class ProxyAuthenticationTest extends ProducerConsumerBase {
    
    private int port;
    private ProxyServer proxyServer;
    private WebSocketService service;

    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        port = PortManager.nextFreePort();
        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(port);
        config.setClusterName("use");
        config.setAuthenticationEnabled(true);
        config.setAuthenticationProviders(
                Sets.newHashSet("com.yahoo.pulsar.websocket.proxy.MockAuthenticationProvider"));
        service = spy(new WebSocketService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
        service.close();
        proxyServer.stop();
        log.info("Finished Cleaning Up Test setup");

    }

    @Test(timeOut=10000)
    public void socketTest() throws InterruptedException {
        String consumerUri = "ws://localhost:" + port
                + "/ws/consumer/persistent/my-property/use/my-ns/my-topic/my-sub";
        String producerUri = "ws://localhost:" + port
                + "/ws/producer/persistent/my-property/use/my-ns/my-topic/";
        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            log.info("Connecting to : {}", consumeUri);

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            // let it connect
            Thread.sleep(1000);
            Assert.assertTrue(consumerFuture.get().isOpen());
            Assert.assertTrue(producerFuture.get().isOpen());

            consumeSocket.awaitClose(1, TimeUnit.SECONDS);
            produceSocket.awaitClose(1, TimeUnit.SECONDS);
            Assert.assertTrue(produceSocket.getBuffer().size() > 0);
            Assert.assertEquals(produceSocket.getBuffer(), consumeSocket.getBuffer());
        } catch (Throwable t) {
            log.error(t.getMessage());
        } finally {
            ExecutorService executor = newFixedThreadPool(1);
            try {
                executor.submit(() -> {
                    try {
                        consumeClient.stop();
                        produceClient.stop();
                        log.info("proxy clients are stopped successfully");
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }).get(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("failed to close clients ", e);
            }
            executor.shutdownNow();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyAuthenticationTest.class);
}
