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
package org.apache.pulsar.tests.integration.auth.token;

import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.Collections;
import java.util.EnumSet;
import java.util.stream.Stream;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.ITest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Slf4j
public abstract class PulsarTokenAuthenticationBaseSuite extends PulsarClusterTestBase implements ITest {

    protected String superUserAuthToken;
    protected String clientAuthToken;

    protected abstract void createKeysAndTokens(PulsarContainer container) throws Exception;

    protected abstract void configureBroker(BrokerContainer brokerContainer) throws Exception;

    @BeforeSuite
    @Override
    public void setupCluster() throws Exception {
        // Before starting the cluster, generate the secret key and the token
        // Use Zk container to have 1 container available before starting the cluster
        try (ZKContainer zkContainer = new ZKContainer<>("cli-setup")) {
            zkContainer
                    .withEnv("zkServers", ZKContainer.NAME);
            zkContainer.start();

            createKeysAndTokens(zkContainer);
        }

        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .clusterName(clusterName)
                .build();

        log.info("Setting up cluster {} with token authentication  and {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);

        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            configureBroker(brokerContainer);
            brokerContainer.withEnv("authenticationEnabled", "true");
            brokerContainer.withEnv("authenticationProviders",
                    "org.apache.pulsar.broker.authentication.AuthenticationToken");
            brokerContainer.withEnv("authorizationEnabled", "true");
            brokerContainer.withEnv("superUserRoles", "super-user");
        }

        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    @AfterSuite
    @Override
    public void tearDownCluster() {
        super.tearDownCluster();
    }

    @Override
    public String getTestName() {
        return "token-auth-test-suite";
    }

    @Test
    public void testPublishWithTokenAuth() throws Exception {
        final String tenant = "token-test-tenant" + randomName(4);
        final String namespace = tenant + "/ns-1";
        final String topic = "persistent://" + namespace + "/topic-1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .authentication(AuthenticationFactory.token(superUserAuthToken))
                .build();

        admin.tenants().createTenant(tenant,
                new TenantInfo(Collections.singleton("regular-user"),
                        Collections.singleton(pulsarCluster.getClusterName())));

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));
        admin.namespaces().grantPermissionOnNamespace(namespace, "regular-user", EnumSet.allOf(AuthAction.class));

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .authentication(AuthenticationFactory.token(clientAuthToken))
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING).topic(topic)
                .create();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("my-sub")
                .subscribe();

        final int numMessages = 10;

        for (int i = 0; i < numMessages; i++) {
            producer.send("hello-" + i);
        }

        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive();
            assertEquals(msg.getValue(), "hello-" + i);

            consumer.acknowledge(msg);
        }

        // Test client with no auth and expect it to fail
        @Cleanup
        PulsarClient clientNoAuth = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        try {
            client.newProducer(Schema.STRING).topic(topic)
                    .create();
            fail("Should have failed to create producer");
        } catch (PulsarClientException e) {
            // Expected
        }
    }
}
