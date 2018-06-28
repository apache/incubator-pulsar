/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar;

import com.google.common.collect.Sets;
import lombok.Builder;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Builder(toBuilder = true)
public class EmbeddedPulsar {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedPulsar.class);

    private PulsarService broker;
    private PulsarAdmin admin;
    private LocalBookkeeperEnsemble bkEnsemble;
    private ServiceConfiguration config;
    private WorkerService fnWorkerService;
    private String configFile;

    @Builder.Default
    private boolean wipeData = true;
    @Builder.Default
    private int numOfBk = 1;
    @Builder.Default
    private int zkPort = 2181;
    @Builder.Default
    private int bkPort = 3181;
    @Builder.Default
    private String zkDir = "data/standalone/zookeeper";
    @Builder.Default
    private String bkDir = "data/standalone/bookkeeper";
    @Builder.Default
    private boolean noBroker = false;
    @Builder.Default
    private boolean onlyBroker = false;
    @Builder.Default
    private String advertisedAddress = null;

    public void start() throws Exception {

        this.config = PulsarConfigurationLoader.create((ClassLoader.class.getResourceAsStream("/embedded.conf")), ServiceConfiguration.class);

        String zkServers = "127.0.0.1";

        if (advertisedAddress != null) {
            // Use advertised address from command line
            config.setAdvertisedAddress(advertisedAddress);
            zkServers = advertisedAddress;
        } else if (isBlank(config.getAdvertisedAddress())) {
            // Use advertised address as local hostname
            config.setAdvertisedAddress(ServiceConfigurationUtils.unsafeLocalhostResolve());
        } else {
            // Use advertised address from config file
        }

        // Set ZK server's host to localhost
        config.setZookeeperServers(zkServers + ":" + zkPort);
        config.setConfigurationStoreServers(zkServers + ":" + zkPort);
        config.setRunningStandalone(true);

        if (config == null) {
            System.exit(1);
        }

        log.debug("--- setup PulsarStandaloneStarter ---");

        if (!onlyBroker) {
            // Start LocalBookKeeper
            bkEnsemble = new LocalBookkeeperEnsemble(numOfBk, zkPort, bkPort, zkDir, bkDir, wipeData, advertisedAddress);
            bkEnsemble.startStandalone();
        }

        if (noBroker) {
            return;
        }

        // Start Broker
        broker = new PulsarService(config, Optional.ofNullable(fnWorkerService));
        broker.start();

        URL webServiceUrl = new URL(
            String.format("http://%s:%d", config.getAdvertisedAddress(), config.getWebServicePort()));
        final String brokerServiceUrl = String.format("pulsar://%s:%d", config.getAdvertisedAddress(),
            config.getBrokerServicePort());
        admin = PulsarAdmin.builder().serviceHttpUrl(webServiceUrl.toString()).authentication(
            config.getBrokerClientAuthenticationPlugin(), config.getBrokerClientAuthenticationParameters()).build();

        // Create a sample namespace
        final String property = "sample";
        final String cluster = config.getClusterName();
        final String globalCluster = "global";
        final String namespace = property + "/" + cluster + "/ns1";
        try {
            ClusterData clusterData = new ClusterData(webServiceUrl.toString(), null /* serviceUrlTls */,
                brokerServiceUrl, null /* brokerServiceUrlTls */);
            if (!admin.clusters().getClusters().contains(cluster)) {
                admin.clusters().createCluster(cluster, clusterData);
            } else {
                admin.clusters().updateCluster(cluster, clusterData);
            }

            // Create marker for "global" cluster
            if (!admin.clusters().getClusters().contains(globalCluster)) {
                admin.clusters().createCluster(globalCluster, new ClusterData(null, null));
            }

            if (!admin.tenants().getTenants().contains(property)) {
                admin.tenants().createTenant(property,
                    new TenantInfo(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }

            if (!admin.namespaces().getNamespaces(property).contains(namespace)) {
                admin.namespaces().createNamespace(namespace);
            }
        } catch (PulsarAdminException e) {
            log.info(e.getMessage());
        }

        // Create a public tenant and default namespace
        final String publicTenant = TopicName.PUBLIC_TENANT;
        final String defaultNamespace = TopicName.PUBLIC_TENANT + "/" + TopicName.DEFAULT_NAMESPACE;
        try {
            if (!admin.tenants().getTenants().contains(publicTenant)) {
                admin.tenants().createTenant(publicTenant,
                    new TenantInfo(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }
            if (!admin.namespaces().getNamespaces(publicTenant).contains(defaultNamespace)) {
                admin.namespaces().createNamespace(defaultNamespace);
                admin.namespaces().setNamespaceReplicationClusters(defaultNamespace, Sets.newHashSet(config.getClusterName()));
            }
        } catch (PulsarAdminException e) {
            log.info(e.getMessage());
        }

        log.debug("--- setup completed ---");
    }


    public void stop() {
        try {
            if (fnWorkerService != null) {
                fnWorkerService.stop();
            }

            if (broker != null) {
                broker.close();
            }

            if (bkEnsemble != null) {
                bkEnsemble.stop();
            }
        } catch (Exception e) {
            log.error("Shutdown failed: {}", e.getMessage());
        }
    }


    public static void main(String[] args) throws Exception {
        EmbeddedPulsar embeddedPulsar = EmbeddedPulsar.builder().build();
        embeddedPulsar.start();
        embeddedPulsar.stop();
    }
}