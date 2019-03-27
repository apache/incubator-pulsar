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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.EnsemblePlacementPolicyConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 */
public class BrokerBookieIsolationTest {

    private LocalBookkeeperEnsemble bkEnsemble;
    private PulsarService pulsarService;

    private final int ZOOKEEPER_PORT = PortManager.nextFreePort();
    private final int PRIMARY_BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
    private final int PRIMARY_BROKER_PORT = PortManager.nextFreePort();

    private static final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();

    @BeforeMethod
    protected void setup() throws Exception {
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(4, ZOOKEEPER_PORT, () -> PortManager.nextFreePort());
        bkEnsemble.start();
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        if (pulsarService != null) {
            pulsarService.close();
        }
        bkEnsemble.stop();
    }

    /**
     * Validate that broker can support tenant based bookie isolation.
     * 
     * <pre>
     * 1. create two bookie-info group : default-group and isolated-group
     * 2. namespace ns1 : uses default-group 
     *    validate: bookie-ensemble for ns1-topics's ledger will be from default-group 
     * 3. namespace ns2,ns3,ns4: uses isolated-group
     *    validate: bookie-ensemble for above namespace-topics's ledger will be from isolated-group
     * </pre>
     * 
     * @throws Exception
     */
    @Test
    public void testBookieIsolation() throws Exception {

        final String tenant1 = "tenant1";
        final String cluster = "use";
        final String ns1 = String.format("%s/%s/%s", tenant1, cluster, "ns1");
        final String ns2 = String.format("%s/%s/%s", tenant1, cluster, "ns2");
        final String ns3 = String.format("%s/%s/%s", tenant1, cluster, "ns3");
        final String ns4 = String.format("%s/%s/%s", tenant1, cluster, "ns4");
        final int totalPublish = 100;

        final String brokerBookkeeperClientIsolationGroups = "default-group";
        final String tenantNamespaceIsolationGroups = "tenant1-isolation";

        BookieServer[] bookies = bkEnsemble.getBookies();
        ZooKeeper zkClient = bkEnsemble.getZkClient();

        Set<BookieSocketAddress> defaultBookies = Sets.newHashSet(bookies[0].getLocalAddress(),
                bookies[1].getLocalAddress());
        Set<BookieSocketAddress> isolatedBookies = Sets.newHashSet(bookies[2].getLocalAddress(),
                bookies[3].getLocalAddress());

        setDefaultIsolationGroup(brokerBookkeeperClientIsolationGroups, zkClient, defaultBookies);
        setDefaultIsolationGroup(tenantNamespaceIsolationGroups, zkClient, isolatedBookies);

        ServiceConfiguration config = new ServiceConfiguration();
        config.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config.setClusterName(cluster);
        config.setWebServicePort(Optional.of(PRIMARY_BROKER_WEBSERVICE_PORT));
        config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config.setBrokerServicePort(Optional.of(PRIMARY_BROKER_PORT));
        config.setAdvertisedAddress("localhost");
        config.setBookkeeperClientIsolationGroups(brokerBookkeeperClientIsolationGroups);

        config.setManagedLedgerDefaultEnsembleSize(2);
        config.setManagedLedgerDefaultWriteQuorum(2);
        config.setManagedLedgerDefaultAckQuorum(2);

        int totalEntriesPerLedger = 20;
        int totalLedgers = totalPublish / totalEntriesPerLedger;
        config.setManagedLedgerMaxEntriesPerLedger(totalEntriesPerLedger);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        pulsarService = new PulsarService(config);
        pulsarService.start();

        URL brokerUrl = new URL("http://127.0.0.1" + ":" + PRIMARY_BROKER_WEBSERVICE_PORT);
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).build();

        ClusterData clusterData = new ClusterData(pulsarService.getWebServiceAddress());
        admin.clusters().createCluster(cluster, clusterData);
        TenantInfo tenantInfo = new TenantInfo(null, Sets.newHashSet(cluster));
        admin.tenants().createTenant(tenant1, tenantInfo);
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);
        admin.namespaces().createNamespace(ns3);
        admin.namespaces().createNamespace(ns4);
        admin.namespaces().setBookieAffinityGroup(ns2, tenantNamespaceIsolationGroups);
        admin.namespaces().setBookieAffinityGroup(ns3, tenantNamespaceIsolationGroups);
        admin.namespaces().setBookieAffinityGroup(ns4, tenantNamespaceIsolationGroups);

        assertEquals(admin.namespaces().getBookieAffinityGroup(ns2), tenantNamespaceIsolationGroups);
        assertEquals(admin.namespaces().getBookieAffinityGroup(ns3), tenantNamespaceIsolationGroups);
        assertEquals(admin.namespaces().getBookieAffinityGroup(ns4), tenantNamespaceIsolationGroups);

        try {
            admin.namespaces().getBookieAffinityGroup(ns1);
        } catch (PulsarAdminException.NotFoundException e) {
            // Ok
        }

        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(brokerUrl.toString())
                .statsInterval(-1, TimeUnit.SECONDS).build();

        PersistentTopic topic1 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns1, "topic1", totalPublish);
        PersistentTopic topic2 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns2, "topic1", totalPublish);
        PersistentTopic topic3 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns3, "topic1", totalPublish);
        PersistentTopic topic4 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns4, "topic1", totalPublish);

        Bookie bookie1 = bookies[0].getBookie();
        Field ledgerManagerField = Bookie.class.getDeclaredField("ledgerManager");
        ledgerManagerField.setAccessible(true);
        LedgerManager ledgerManager = (LedgerManager) ledgerManagerField.get(bookie1);

        // namespace: ns1
        ManagedLedgerImpl ml = (ManagedLedgerImpl) topic1.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), defaultBookies);

        // namespace: ns2
        ml = (ManagedLedgerImpl) topic2.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), isolatedBookies);

        // namespace: ns3
        ml = (ManagedLedgerImpl) topic3.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), isolatedBookies);

        // namespace: ns4
        ml = (ManagedLedgerImpl) topic4.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), isolatedBookies);

        ManagedLedgerClientFactory mlFactory = pulsarService.getManagedLedgerClientFactory();
        Map<EnsemblePlacementPolicyConfig, BookKeeper> bkPlacementPolicyToBkClientMap = mlFactory
                .getBkEnsemblePolicyToBookKeeperMap();

        // broker should create only 1 bk-client and factory per isolation-group
        assertEquals(bkPlacementPolicyToBkClientMap.size(), 1);
        Class<? extends EnsemblePlacementPolicy> clazz = bkPlacementPolicyToBkClientMap.keySet().iterator().next()
                .getPolicyClass();
        System.out.println(clazz);

    }

    private void assertAffinityBookies(LedgerManager ledgerManager, List<LedgerInfo> ledgers1,
            Set<BookieSocketAddress> defaultBookies) throws Exception {
        for (LedgerInfo lInfo : ledgers1) {
            long ledgerId = lInfo.getLedgerId();
            CompletableFuture<Versioned<LedgerMetadata>> ledgerMetaFuture = ledgerManager.readLedgerMetadata(ledgerId);
            LedgerMetadata ledgerMetadata = ledgerMetaFuture.get().getValue();
            Set<BookieSocketAddress> ledgerBookies = Sets.newHashSet();
            ledgerBookies.addAll(ledgerMetadata.getAllEnsembles().values().iterator().next());
            assertEquals(ledgerBookies.size(), defaultBookies.size());
            ledgerBookies.removeAll(defaultBookies);
            assertEquals(ledgerBookies.size(), 0);
        }
    }

    private Topic createTopicAndPublish(PulsarClient pulsarClient, String ns, String topicLocalName, int totalPublish)
            throws Exception {
        final String topicName = String.format("persistent://%s/%s", ns, topicLocalName);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscribe();
        consumer.close();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < totalPublish; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.close();

        return pulsarService.getBrokerService().getTopicReference(topicName).get();
    }

    private void setDefaultIsolationGroup(String brokerBookkeeperClientIsolationGroups, ZooKeeper zkClient,
            Set<BookieSocketAddress> bookieAddresses) throws Exception {
        BookiesRackConfiguration bookies = null;
        try {
            byte[] data = zkClient.getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null);
            System.out.println(new String(data));
            bookies = jsonMapper.readValue(data, BookiesRackConfiguration.class);
        } catch (KeeperException.NoNodeException e) {
            // Ok.. create new bookie znode
            zkClient.create(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, "".getBytes(), Acl,
                    CreateMode.PERSISTENT);
        }
        if (bookies == null) {
            bookies = new BookiesRackConfiguration();
        }

        Map<String, BookieInfo> bookieInfoMap = Maps.newHashMap();
        for (BookieSocketAddress bkSocket : bookieAddresses) {
            BookieInfo info = new BookieInfo("use", bkSocket.getHostName() + ":" + bkSocket.getPort());
            bookieInfoMap.put(bkSocket.toString(), info);
        }
        bookies.put(brokerBookkeeperClientIsolationGroups, bookieInfoMap);

        zkClient.setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookies), -1);
    }

    private static final Logger log = LoggerFactory.getLogger(BrokerBookieIsolationTest.class);

}
