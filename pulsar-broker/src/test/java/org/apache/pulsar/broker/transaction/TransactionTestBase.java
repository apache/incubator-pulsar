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
package org.apache.pulsar.broker.transaction;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import org.apache.pulsar.broker.intercept.CounterBrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

@Slf4j
public class TransactionTestBase {

    private final static String CLUSTER_NAME = "test";

    @Setter
    private int brokerCount = 3;

    private final List<SameThreadOrderedSafeExecutor> orderedExecutorList = new ArrayList<>();
    @Getter
    private final List<ServiceConfiguration> serviceConfigurationList = new ArrayList<>();
    @Getter
    private final List<PulsarService> pulsarServiceList = new ArrayList<>();

    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;

    private MockZooKeeper mockZooKeeper;
    private ExecutorService bkExecutor;
    private NonClosableMockBookKeeper mockBookKeeper;

    public void internalSetup() throws Exception {
        init();

        int webServicePort = serviceConfigurationList.get(0).getWebServicePort().get();
        admin = spy(PulsarAdmin.builder().serviceHttpUrl("http://localhost:" + webServicePort).build());

        int brokerPort = serviceConfigurationList.get(0).getBrokerServicePort().get();
        pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:" + brokerPort).build();
    }

    private void init() throws Exception {
        mockZooKeeper = createMockZooKeeper();

        bkExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("mock-pulsar-bk")
                        .setUncaughtExceptionHandler((thread, ex) -> log.info("Uncaught exception", ex))
                        .build());
        mockBookKeeper = createMockBookKeeper(mockZooKeeper, bkExecutor);
        startBroker();
    }

    protected void startBroker() throws Exception {
        for (int i = 0; i < brokerCount; i++) {
            ServiceConfiguration conf = new ServiceConfiguration();
            conf.setClusterName(CLUSTER_NAME);
            conf.setAdvertisedAddress("localhost");
            conf.setManagedLedgerCacheSizeMB(8);
            conf.setActiveConsumerFailoverDelayTimeMillis(0);
            conf.setDefaultNumberOfNamespaceBundles(1);
            conf.setZookeeperServers("localhost:2181");
            conf.setConfigurationStoreServers("localhost:3181");
            conf.setAllowAutoTopicCreationType("non-partitioned");
            conf.setBookkeeperClientExposeStatsToPrometheus(true);
            conf.setAcknowledgmentAtBatchIndexLevelEnabled(true);

            Integer brokerServicePort = PortManager.nextFreePort();
            conf.setBrokerServicePort(Optional.of(brokerServicePort));

            conf.setBrokerServicePortTls(Optional.of(PortManager.nextFreePort()));
            conf.setAdvertisedAddress("localhost");
            conf.setWebServicePort(Optional.of(PortManager.nextFreePort()));
            conf.setWebServicePortTls(Optional.of(PortManager.nextFreePort()));
            serviceConfigurationList.add(conf);

            PulsarService pulsar = spy(new PulsarService(conf));

            setupBrokerMocks(pulsar);
            pulsar.start();
            pulsarServiceList.add(pulsar);
        }
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockZooKeeperClientFactory).when(pulsar).getZooKeeperClientFactory();
        doReturn(mockBookKeeperClientFactory).when(pulsar).newBookKeeperClientFactory();

        Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
        doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

        SameThreadOrderedSafeExecutor executor = new SameThreadOrderedSafeExecutor();
        orderedExecutorList.add(executor);
        doReturn(executor).when(pulsar).getOrderedExecutor();
        doReturn(new CounterBrokerInterceptor()).when(pulsar).getBrokerInterceptor();

        doAnswer((invocation) -> spy(invocation.callRealMethod())).when(pulsar).newCompactor();
    }

    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList,
                CreateMode.PERSISTENT);
        return zk;
    }

    public static TransactionTestBase.NonClosableMockBookKeeper createMockBookKeeper(ZooKeeper zookeeper,
                                                                                             ExecutorService executor) throws Exception {
        return spy(new TransactionTestBase.NonClosableMockBookKeeper(zookeeper, executor));
    }

    // Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test
    public static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(ZooKeeper zk, ExecutorService executor) throws Exception {
            super(zk, executor);
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdown() {
            super.shutdown();
        }
    }

    protected ZooKeeperClientFactory mockZooKeeperClientFactory = new ZooKeeperClientFactory() {

        @Override
        public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                                                   int zkSessionTimeoutMillis) {
            // Always return the same instance (so that we don't loose the mock ZK content on broker restart
            return CompletableFuture.completedFuture(mockZooKeeper);
        }
    };

    private final BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {

        @Override
        public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                 Map<String, Object> properties) {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }

        @Override
        public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                 Map<String, Object> properties, StatsLogger statsLogger) {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }

        @Override
        public void close() {
            // no-op
        }
    };

    protected final void internalCleanup() {
        try {
            // if init fails, some of these could be null, and if so would throw
            // an NPE in shutdown, obscuring the real error
            if (admin != null) {
                admin.close();
                admin = null;
            }
            if (pulsarClient != null) {
                pulsarClient.shutdown();
                pulsarClient = null;
            }
            if (pulsarServiceList.size() > 0) {
                for (PulsarService pulsarService : pulsarServiceList) {
                    pulsarService.close();
                }
            }
            if (mockBookKeeper != null) {
                mockBookKeeper.reallyShutdown();
            }
            if (mockZooKeeper != null) {
                mockZooKeeper.shutdown();
            }
            if (orderedExecutorList.size() > 0) {
                for (int i = 0; i < orderedExecutorList.size(); i++) {
                    SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor = orderedExecutorList.get(i);
                    if(sameThreadOrderedSafeExecutor != null) {
                        try {
                            sameThreadOrderedSafeExecutor.shutdownNow();
                            sameThreadOrderedSafeExecutor.awaitTermination(5, TimeUnit.SECONDS);
                        } catch (InterruptedException ex) {
                            log.error("sameThreadOrderedSafeExecutor shutdown had error", ex);
                            Thread.currentThread().interrupt();
                        }
                        orderedExecutorList.set(i, null);
                    }
                }
            }
            if(bkExecutor != null) {
                try {
                    bkExecutor.shutdownNow();
                    bkExecutor.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    log.error("bkExecutor shutdown had error", ex);
                    Thread.currentThread().interrupt();
                }
                bkExecutor = null;
            }
        } catch (Exception e) {
            log.warn("Failed to clean up mocked pulsar service:", e);
        }
    }

}
