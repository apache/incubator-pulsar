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
package org.apache.pulsar.common.policies.data;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

public class Policies {
    public enum PolicyProperty {
        BacklogQuotaMap(new TypeToken<Map<BacklogQuota.BacklogQuotaType, BacklogQuota>>(){}.getType(), "backlog_quota_map", false),
        ClusterDispatchRate(new TypeToken<Map<String, DispatchRate>>(){}.getType(), "clusterDispatchRate", true),
        DeduplicationEnabled(new TypeToken<Boolean>(){}.getType(), "deduplicationEnabled", false),
        MessageTtlInSeconds(new TypeToken<Integer>(){}.getType(), "message_ttl_in_seconds", false),
        RetentionPolicies(new TypeToken<RetentionPolicies>(){}.getType(), "retention_policies", false),
        AntiAffinityGroup(new TypeToken<String>(){}.getType(), "antiAffinityGroup", false),
        EncryptionRequired(new TypeToken<Boolean>(){}.getType(), "encryption_required", false),
        SubscriptionAuthMode(new TypeToken<SubscriptionAuthMode>(){}.getType(), "subscription_auth_mode", false),
        MaxProducersPerTopic(new TypeToken<Integer>(){}.getType(), "max_producers_per_topic", true),
        MaxConsumersPerTopic(new TypeToken<Integer>(){}.getType(), "max_consumers_per_topic", true),
        MaxConsumersPerSubscription(new TypeToken<Integer>(){}.getType(), "max_consumers_per_subscription", true),
        ;

        private final Type type;
        private final String propertyName;
        private final boolean isOnlySuperUser;

        PolicyProperty(final Type type, final String propertyName, boolean isOnlySuperUser) {
            this.type = type;
            this.propertyName = propertyName;
            this.isOnlySuperUser = isOnlySuperUser;
        }

        public Type getType() {
            return type;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public boolean isOnlySuperUser() {
            return isOnlySuperUser;
        }
    }

    public final AuthPolicies auth_policies = new AuthPolicies();
    public List<String> replication_clusters = Lists.newArrayList();
    public BundlesData bundles = defaultBundle();
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlog_quota_map = Maps.newHashMap();
    public Map<String, DispatchRate> clusterDispatchRate = Maps.newHashMap();
    public PersistencePolicies persistence = null;

    // If set, it will override the broker settings for enabling deduplication
    public Boolean deduplicationEnabled = null;

    public Map<String, Integer> latency_stats_sample_rate = Maps.newHashMap();
    public int message_ttl_in_seconds = 0;
    public RetentionPolicies retention_policies = null;
    public boolean deleted = false;
    public String antiAffinityGroup;

    public static final String FIRST_BOUNDARY = "0x00000000";
    public static final String LAST_BOUNDARY = "0xffffffff";

    public boolean encryption_required = false;
    public SubscriptionAuthMode subscription_auth_mode = SubscriptionAuthMode.None;

    public int max_producers_per_topic = 0;
    public int max_consumers_per_topic = 0;
    public int max_consumers_per_subscription = 0;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Policies) {
            Policies other = (Policies) obj;
            return Objects.equals(auth_policies, other.auth_policies)
                    && Objects.equals(replication_clusters, other.replication_clusters)
                    && Objects.equals(backlog_quota_map, other.backlog_quota_map)
                    && Objects.equals(clusterDispatchRate, other.clusterDispatchRate)
                    && Objects.equals(deduplicationEnabled, other.deduplicationEnabled)
                    && Objects.equals(persistence, other.persistence) && Objects.equals(bundles, other.bundles)
                    && Objects.equals(latency_stats_sample_rate, other.latency_stats_sample_rate)
                    && message_ttl_in_seconds == other.message_ttl_in_seconds
                    && Objects.equals(retention_policies, other.retention_policies)
                    && Objects.equals(encryption_required, other.encryption_required)
                    && Objects.equals(subscription_auth_mode, other.subscription_auth_mode)
                    && Objects.equals(antiAffinityGroup, other.antiAffinityGroup)
                    && max_producers_per_topic == other.max_producers_per_topic
                    && max_consumers_per_topic == other.max_consumers_per_topic
                    && max_consumers_per_subscription == other.max_consumers_per_subscription;
        }

        return false;
    }

    public static BundlesData defaultBundle() {
        BundlesData bundle = new BundlesData(1);
        List<String> boundaries = Lists.newArrayList();
        boundaries.add(FIRST_BOUNDARY);
        boundaries.add(LAST_BOUNDARY);
        bundle.setBoundaries(boundaries);
        return bundle;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("auth_policies", auth_policies)
                .add("replication_clusters", replication_clusters).add("bundles", bundles)
                .add("backlog_quota_map", backlog_quota_map).add("persistence", persistence)
                .add("deduplicationEnabled", deduplicationEnabled)
                .add("clusterDispatchRate", clusterDispatchRate)
                .add("latency_stats_sample_rate", latency_stats_sample_rate)
                .add("antiAffinityGroup", antiAffinityGroup)
                .add("message_ttl_in_seconds", message_ttl_in_seconds).add("retention_policies", retention_policies)
                .add("deleted", deleted)
                .add("encryption_required", encryption_required)
                .add("subscription_auth_mode", subscription_auth_mode)
                .add("max_producers_per_topic", max_producers_per_topic)
                .add("max_consumers_per_topic", max_consumers_per_topic)
                .add("max_consumers_per_subscription", max_consumers_per_topic).toString();
    }
}
