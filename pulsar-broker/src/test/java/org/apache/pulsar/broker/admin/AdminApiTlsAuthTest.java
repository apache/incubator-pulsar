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
package org.apache.pulsar.broker.admin;

import com.google.common.collect.ImmutableSet;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

import javax.net.ssl.SSLContext;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import lombok.extern.slf4j.Slf4j;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import javax.servlet.http.HttpServletResponse;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.JacksonConfigurator;
import org.apache.pulsar.client.api.BrokerServiceLookupTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.tls.NoopHostnameVerifier;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.SecurityUtility;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class AdminApiTlsAuthTest extends MockedPulsarServiceBaseTest {

    private static String getTLSFile(String name) {
        return String.format("./src/test/resources/authentication/tls-http/%s.pem", name);
    }

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(getTLSFile("broker.cert"));
        conf.setTlsKeyFilePath(getTLSFile("broker.key-pk8"));
        conf.setTlsTrustCertsFilePath(getTLSFile("ca.cert"));
        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(
                ImmutableSet.of("org.apache.pulsar.broker.authentication.AuthenticationProviderTls"));
        conf.setSuperUserRoles(ImmutableSet.of("admin", "superproxy"));
        conf.setProxyRoles(ImmutableSet.of("proxy", "superproxy"));
        conf.setAuthorizationEnabled(true);

        conf.setBrokerClientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationTls");
        conf.setBrokerClientAuthenticationParameters(
                String.format("tlsCertFile:%s,tlsKeyFile:%s", getTLSFile("admin.cert"), getTLSFile("admin.key-pk8")));
        conf.setBrokerClientTrustCertsFilePath(getTLSFile("ca.cert"));
        conf.setBrokerClientTlsEnabled(true);
        conf.setNumExecutorThreadPoolSize(5);
        conf.setStatusCheckServicePort(Optional.of(PortManager.nextFreePort()));

        File statusFile = File.createTempFile("statusFile", ".txt");
        conf.setStatusFilePath(statusFile.getAbsolutePath());

        super.internalSetup();

        PulsarAdmin admin = buildAdminClient("admin");
        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        admin.close();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        // delete temp status file
        File statusFile = new File(conf.getStatusFilePath());
        statusFile.delete();
    }

    WebTarget buildWebClient(String user) throws Exception {
        ClientConfig httpConfig = new ClientConfig();
        httpConfig.property(ClientProperties.FOLLOW_REDIRECTS, true);
        httpConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 8);
        httpConfig.register(MultiPartFeature.class);

        ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(httpConfig)
                .register(JacksonConfigurator.class).register(JacksonFeature.class);

        X509Certificate trustCertificates[] = SecurityUtility.loadCertificatesFromPemFile(getTLSFile("ca.cert"));
        SSLContext sslCtx = SecurityUtility.createSslContext(false, trustCertificates,
                SecurityUtility.loadCertificatesFromPemFile(getTLSFile(user + ".cert")),
                SecurityUtility.loadPrivateKeyFromPemFile(getTLSFile(user + ".key-pk8")));
        clientBuilder.sslContext(sslCtx).hostnameVerifier(NoopHostnameVerifier.INSTANCE);
        Client client = clientBuilder.build();

        return client.target(brokerUrlTls.toString());
    }

    PulsarAdmin buildAdminClient(String user) throws Exception {
        return PulsarAdmin.builder().allowTlsInsecureConnection(false).enableTlsHostnameVerification(false)
                .serviceHttpUrl(brokerUrlTls.toString())
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls", String.format(
                        "tlsCertFile:%s,tlsKeyFile:%s", getTLSFile(user + ".cert"), getTLSFile(user + ".key-pk8")))
                .tlsTrustCertsFilePath(getTLSFile("ca.cert")).build();
    }

    PulsarClient buildClient(String user) throws Exception {
        return PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrlTls()).enableTlsHostnameVerification(false)
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls", String.format(
                        "tlsCertFile:%s,tlsKeyFile:%s", getTLSFile(user + ".cert"), getTLSFile(user + ".key-pk8")))
                .tlsTrustCertsFilePath(getTLSFile("ca.cert")).build();
    }

    @Test
    public void testSuperUserCanListTenants() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("foobar"), ImmutableSet.of("test")));
            Assert.assertEquals(ImmutableSet.of("tenant1"), admin.tenants().getTenants());
        }
    }

    @Test
    public void testProxyRoleCantListTenants() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("foobar"), ImmutableSet.of("test")));
        }
        try (PulsarAdmin admin = buildAdminClient("proxy")) {
            admin.tenants().getTenants();
            Assert.fail("Shouldn't be able to list tenants");
        } catch (PulsarAdminException.NotAuthorizedException e) {
            // expected
        }
    }

    @Test
    public void testProxyRoleCantListNamespacesEvenWithAccess() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("proxy"), ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
        }
        try (PulsarAdmin admin = buildAdminClient("proxy")) {
            admin.namespaces().getNamespaces("tenant1");
            Assert.fail("Shouldn't be able to list namespaces");
        } catch (PulsarAdminException.NotAuthorizedException e) {
            // expected
        }
    }

    @Test
    public void testAuthorizedUserAsOriginalPrincipal() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1",
                    new TenantInfo(ImmutableSet.of("proxy", "user1"), ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
        }
        WebTarget root = buildWebClient("proxy");
        Assert.assertEquals(ImmutableSet.of("tenant1/ns1"),
                root.path("/admin/v2/namespaces").path("tenant1").request(MediaType.APPLICATION_JSON)
                        .header("X-Original-Principal", "user1").get(new GenericType<List<String>>() {
                        }));
    }

    @Test
    public void testUnauthorizedUserAsOriginalPrincipal() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1",
                    new TenantInfo(ImmutableSet.of("proxy", "user1"), ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
        }
        WebTarget root = buildWebClient("proxy");
        try {
            root.path("/admin/v2/namespaces").path("tenant1").request(MediaType.APPLICATION_JSON)
                    .header("X-Original-Principal", "user2").get(new GenericType<List<String>>() {
                    });
            Assert.fail("user2 should not be authorized");
        } catch (NotAuthorizedException e) {
            // expected
        }
    }

    @Test
    public void testAuthorizedUserAsOriginalPrincipalButProxyNotAuthorized() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("user1"), ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
        }
        WebTarget root = buildWebClient("proxy");
        try {
            root.path("/admin/v2/namespaces").path("tenant1").request(MediaType.APPLICATION_JSON)
                    .header("X-Original-Principal", "user1").get(new GenericType<List<String>>() {
                    });
            Assert.fail("Shouldn't be able to list namespaces");
        } catch (NotAuthorizedException e) {
            // expected
        }
    }

    @Test
    public void testAuthorizedUserAsOriginalPrincipalProxyIsSuperUser() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("user1"), ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
        }
        WebTarget root = buildWebClient("superproxy");
        Assert.assertEquals(ImmutableSet.of("tenant1/ns1"),
                root.path("/admin/v2/namespaces").path("tenant1").request(MediaType.APPLICATION_JSON)
                        .header("X-Original-Principal", "user1").get(new GenericType<List<String>>() {
                        }));
    }

    @Test
    public void testUnauthorizedUserAsOriginalPrincipalProxyIsSuperUser() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("user1"), ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
        }
        WebTarget root = buildWebClient("superproxy");
        try {
            root.path("/admin/v2/namespaces").path("tenant1").request(MediaType.APPLICATION_JSON)
                    .header("X-Original-Principal", "user2").get(new GenericType<List<String>>() {
                    });
            Assert.fail("user2 should not be authorized");
        } catch (NotAuthorizedException e) {
            // expected
        }
    }

    @Test
    public void testProxyUserViaProxy() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("proxy"), ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
        }
        WebTarget root = buildWebClient("superproxy");
        try {
            root.path("/admin/v2/namespaces").path("tenant1").request(MediaType.APPLICATION_JSON)
                    .header("X-Original-Principal", "proxy").get(new GenericType<List<String>>() {
                    });
            Assert.fail("proxy should not be authorized");
        } catch (NotAuthorizedException e) {
            // expected
        }
    }

    @Test
    public void testSuperProxyUserAndAdminCanListTenants() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("user1"), ImmutableSet.of("test")));
        }
        WebTarget root = buildWebClient("superproxy");
        Assert.assertEquals(ImmutableSet.of("tenant1"),
                root.path("/admin/v2/tenants").request(MediaType.APPLICATION_JSON)
                        .header("X-Original-Principal", "admin").get(new GenericType<List<String>>() {
                        }));
    }

    @Test
    public void testSuperProxyUserAndNonAdminCannotListTenants() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("proxy"), ImmutableSet.of("test")));
        }
        WebTarget root = buildWebClient("superproxy");
        try {
            root.path("/admin/v2/tenants").request(MediaType.APPLICATION_JSON).header("X-Original-Principal", "user1")
                    .get(new GenericType<List<String>>() {
                    });
            Assert.fail("user1 should not be authorized");
        } catch (NotAuthorizedException e) {
            // expected
        }
    }

    @Test
    public void testProxyCannotSetOriginalPrincipalAsEmpty() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("user1"), ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
        }
        WebTarget root = buildWebClient("proxy");
        try {
            root.path("/admin/v2/namespaces").path("tenant1").request(MediaType.APPLICATION_JSON)
                    .header("X-Original-Principal", "").get(new GenericType<List<String>>() {
                    });
            Assert.fail("Proxy shouldn't be able to set original principal.");
        } catch (NotAuthorizedException e) {
            // expected
        }
    }

    // For https://github.com/apache/pulsar/issues/2880
    @Test
    public void testDeleteNamespace() throws Exception {
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            log.info("Creating tenant");
            admin.tenants().createTenant("tenant1", new TenantInfo(ImmutableSet.of("admin"), ImmutableSet.of("test")));
            log.info("Creating namespace, and granting perms to user1");
            admin.namespaces().createNamespace("tenant1/ns1", ImmutableSet.of("test"));
            admin.namespaces().grantPermissionOnNamespace("tenant1/ns1", "user1", ImmutableSet.of(AuthAction.produce));

            log.info("user1 produces some messages");
            try (PulsarClient client = buildClient("user1");
                    Producer<String> producer = client.newProducer(Schema.STRING).topic("tenant1/ns1/foobar")
                            .create()) {
                producer.send("foobar");
            }

            log.info("Deleting the topic");
            admin.topics().delete("tenant1/ns1/foobar", true);

            log.info("Deleting namespace");
            admin.namespaces().deleteNamespace("tenant1/ns1");
        }
    }

    @Test
    public void testVipHealthCheckService() throws Exception {
        AsyncHttpClient httpClient = BrokerServiceLookupTest.getHttpClient("Pulsar-Java-1.20");

        // (1) hit status.html url of statusCheckService port
        URI brokerServiceUrl = new URI("http://localhost:" + conf.getStatusCheckServicePort().get());

        String requestUrl = new URL(brokerServiceUrl.toURL(), "/status.html").toString();
        BoundRequestBuilder builder = httpClient.prepareGet(requestUrl);
        ListenableFuture<Response> responseFuture = builder.setHeader("Accept", "application/json")
                .execute(new AsyncCompletionHandler<Response>() {

                    @Override
                    public Response onCompleted(Response response) throws Exception {
                        return response;
                    }

                    @Override
                    public void onThrowable(Throwable t) {
                        Assert.fail("Vip health status check failed");
                        log.warn("[{}] Failed to perform http request: {}", requestUrl, t.getMessage());
                    }
                });
        Response response = responseFuture.get();
        if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
            log.warn("[{}] HTTP get request failed: {}", requestUrl, response.getStatusText());
            Assert.fail("Vip health status check failed");
        }
        String responseData = new String(response.getResponseBodyAsBytes());
        assertEquals(responseData, "OK");

        // (2) hit admin-url on statusCheckService port
        final String adminUrl = "/admin/v2/tenants/tenant1";
        String requestUrl2 = new URL(brokerServiceUrl.toURL(), adminUrl).toString();
        builder = httpClient.prepareGet(requestUrl2);
        responseFuture = builder.setHeader("Accept", "application/json")
                .execute(new AsyncCompletionHandler<Response>() {

                    @Override
                    public Response onCompleted(Response response) throws Exception {
                        return response;
                    }

                    @Override
                    public void onThrowable(Throwable t) {
                        Assert.fail("Vip health status check failed");
                        log.warn("[{}] Failed to perform http request: {}", requestUrl, t.getMessage());
                    }
                });
        response = responseFuture.get();
        assertEquals(response.getStatusCode(), HttpServletResponse.SC_NOT_FOUND);

        // (3) hit same admin url to non-tls broker service : that cause 401 (unauthorized request)
        String requestUrl3 = new URL(new URL(pulsar.getWebServiceAddress()), adminUrl).toString();
        builder = httpClient.prepareGet(requestUrl3);
        responseFuture = builder.setHeader("Accept", "application/json")
                .execute(new AsyncCompletionHandler<Response>() {

                    @Override
                    public Response onCompleted(Response response) throws Exception {
                        return response;
                    }

                    @Override
                    public void onThrowable(Throwable t) {
                        Assert.fail("Vip health status check failed");
                        log.warn("[{}] Failed to perform http request: {}", requestUrl, t.getMessage());
                    }
                });
        response = responseFuture.get();
        assertEquals(response.getStatusCode(), HttpServletResponse.SC_UNAUTHORIZED);
    }
}
