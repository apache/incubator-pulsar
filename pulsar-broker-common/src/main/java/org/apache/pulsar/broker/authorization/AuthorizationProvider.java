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
package org.apache.pulsar.broker.authorization;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Provider of authorization mechanism
 */
public interface AuthorizationProvider extends Closeable {

    /**
     * Check if specified role is a super user
     * @param role the role to check
     * @return a CompletableFuture containing a boolean in which true means the role is a super user
     * and false if it is not
     */
    default CompletableFuture<Boolean> isSuperUser(String role, ServiceConfiguration serviceConfiguration) {
        Set<String> superUserRoles = serviceConfiguration.getSuperUserRoles();
        return CompletableFuture.completedFuture(role != null && superUserRoles.contains(role) ? true : false);
    }

    /**
     * Check if specified role is a super user
     * @param role the role to check
     * @param authenticationData authentication data related to the role
     * @return a CompletableFuture containing a boolean in which true means the role is a super user
     * and false if it is not
     */
    default CompletableFuture<Boolean> isSuperUser(String role,
                                                   AuthenticationDataSource authenticationData,
                                                   ServiceConfiguration serviceConfiguration) {
        return isSuperUser(role, serviceConfiguration);
    }

    /**
     * Check if specified role is an admin of the tenant
     * @param tenant the tenant to check
     * @param role the role to check
     * @return a CompletableFuture containing a boolean in which true means the role is an admin user
     * and false if it is not
     */
    default CompletableFuture<Boolean> isTenantAdmin(String tenant, String role, TenantInfo tenantInfo,
                                                     AuthenticationDataSource authenticationData) {
        return CompletableFuture.completedFuture(role != null && tenantInfo.getAdminRoles() != null && tenantInfo.getAdminRoles().contains(role) ? true : false);
    }

    /**
     * Perform initialization for the authorization provider
     *
     * @param conf
     *            broker config object
     * @param configCache
     *            pulsar zk configuration cache service
     * @throws IOException
     *             if the initialization fails
     */
    void initialize(ServiceConfiguration conf, ConfigurationCacheService configCache) throws IOException;

    /**
     * Check if the specified role has permission to send messages to the specified fully qualified topic name.
     *
     * @param topicName
     *            the fully qualified topic name associated with the topic.
     * @param role
     *            the app id used to send messages to the topic.
     */
    CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData);

    /**
     * Check if the specified role has permission to receive messages from the specified fully qualified topic name.
     *
     * @param topicName
     *            the fully qualified topic name associated with the topic.
     * @param role
     *            the app id used to receive messages from the topic.
     * @param subscription
     *            the subscription name defined by the client
     */
    CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData, String subscription);

    /**
     * Check whether the specified role can perform a lookup for the specified topic.
     *
     * For that the caller needs to have producer or consumer permission.
     *
     * @param topicName
     * @param role
     * @return
     * @throws Exception
     */
    CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData);

    /**
     * Allow all function operations with in this namespace
     * @param namespaceName The namespace that the function operations can be executed in
     * @param role The role to check
     * @param authenticationData authentication data related to the role
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role,
                                                     AuthenticationDataSource authenticationData);

    /**
     *
     * Grant authorization-action permission on a namespace to the given client
     *
     * @param namespace
     * @param actions
     * @param role
     * @param authDataJson
     *            additional authdata in json format
     * @return CompletableFuture
     * @completesWith <br/>
     *                IllegalArgumentException when namespace not found<br/>
     *                IllegalStateException when failed to grant permission
     */
    CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions, String role,
            String authDataJson);

    /**
     * Grant permission to roles that can access subscription-admin api
     *
     * @param namespace
     * @param subscriptionName
     * @param roles
     * @param authDataJson
     *            additional authdata in json format
     * @return
     */
    CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName, Set<String> roles,
            String authDataJson);

    /**
     * Revoke subscription admin-api access for a role
     * @param namespace
     * @param subscriptionName
     * @param role
     * @return
     */
    CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
            String role, String authDataJson);

    /**
     * Grant authorization-action permission on a topic to the given client
     *
     * @param topicName
     * @param role
     * @param authDataJson
     *            additional authdata in json format
     * @return CompletableFuture
     * @completesWith <br/>
     *                IllegalArgumentException when namespace not found<br/>
     *                IllegalStateException when failed to grant permission
     */
    CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions, String role,
            String authDataJson);

    /**
     * Grant authorization-action permission on a cluster to the given client
     * @param clusterName
     * @param role
     * @param operation
     * @param authData
     * @return CompletableFuture
     * @completesWith <br/>
     *                IllegalArgumentException when topic not found<br/>
     *                IllegalStateException when failed to grant permission
     */
    default CompletableFuture<Boolean> allowClusterOperation(String clusterName, String role,
                                                             ClusterOperation operation,
                                                             AuthenticationDataSource authData) {
        return FutureUtil.failedFuture(
                new IllegalStateException("ClusterOperation is not supported by the Authorization provider you are using."));
    }

    /**
     * Grant authorization-action permission on a tenant to the given client
     * @param tenantName
     * @param originalRole
     * @param role
     * @param operation
     * @param authData
     * @return
     */
    default CompletableFuture<Boolean> allowTenantOperation(String tenantName, String originalRole, String role,
                                                            TenantOperation operation,
                                                            AuthenticationDataSource authData) {
        return FutureUtil.failedFuture(new IllegalStateException(
                String.format("allowTenantOperation(%s) on tenant %s is not supported by the Authorization" +
                                " provider you are using.",
                        operation.toString(), tenantName)));
    }

    /**
     * Grant authorization-action permission on a namespace to the given client
     * @param namespaceName
     * @param originalRole
     * @param role
     * @param operation
     * @param authData
     * @return CompletableFuture
     * @completesWith <br/>
     *                IllegalArgumentException when namespace not found<br/>
     *                IllegalStateException when failed to grant permission
     */
    default CompletableFuture<Boolean> allowNamespaceOperation(NamespaceName namespaceName, String originalRole,
                                                               String role, NamespaceOperation operation,
                                                               AuthenticationDataSource authData) {
        return FutureUtil.failedFuture(
            new IllegalStateException("NamespaceOperation is not supported by the Authorization provider you are using."));
    }

    /**
     * Grant authorization-action permission on a topic to the given client
     * @param topic
     * @param role
     * @param operation
     * @param authData
     * @return CompletableFuture
     * @completesWith <br/>
     *                IllegalArgumentException when topic not found<br/>
     *                IllegalStateException when failed to grant permission
     */
    default CompletableFuture<Boolean> allowTopicOperation(TopicName topic, String role, TopicOperation operation,
                                              AuthenticationDataSource authData) {
        return FutureUtil.failedFuture(
            new IllegalStateException("TopicOperation is not supported by the Authorization provider you are using."));
    }
}
