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
package org.apache.pulsar.broker.loadbalance.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;


/**
 * Load shedding strategy which will attempt to shed exactly one bundle on brokers which are overloaded, that is, whose
 * maximum system resource usage exceeds loadBalancerBrokerOverloadedThresholdPercentage. A bundle will be recommended
 * for unloading off that broker if and only if the following conditions hold: The broker has at least two bundles
 * assigned and the broker has at least one bundle that has not been unloaded recently according to
 * LoadBalancerSheddingGracePeriodMinutes. The unloaded bundle will be the most expensive bundle in terms of message
 * rate that has not been recently unloaded.
 */
@Slf4j
public class OverloadShedder implements LoadSheddingStrategy {

    private final Multimap<String, String> selectedBundlesCache = ArrayListMultimap.create();

    private final static double ADDITIONAL_THRESHOLD_PERCENT_MARGIN = 0.05;

    /**
     * Attempt to shed some bundles off every broker which is overloaded.
     *
     * @param loadData
     *            The load data to used to make the unloading decision.
     * @param conf
     *            The service configuration.
     * @return A map from bundles to unload to the brokers on which they are loaded.
     */
    public Multimap<String, String> findBundlesForUnloading(final LoadData loadData, final ServiceConfiguration conf) {
        selectedBundlesCache.clear();
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();

        // Check every broker and select
        loadData.getBrokerData().forEach((broker, brokerData) -> {

            final LocalBrokerData localData = brokerData.getLocalData();
            final double currentUsage = localData.getMaxResourceUsage();
            if (currentUsage < overloadThreshold) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Broker is not overloaded, ignoring at this point", broker);
                }
                return;
            }

            // We want to offload enough traffic such that this broker will go below the overload threshold
            // Also, add a small margin so that this broker won't be very close to the threshold edge.
            double percentOfTrafficToOffload = currentUsage - overloadThreshold + ADDITIONAL_THRESHOLD_PERCENT_MARGIN;
            double brokerCurrentThroughput = localData.getMsgThroughputIn() + localData.getMsgThroughputOut();

            double minimumThroughputToOffload = brokerCurrentThroughput * percentOfTrafficToOffload;

            log.info(
                    "Attempting to shed load on {}, which has max resource usage above threshold {}% > {}% -- Offloading at least {} MByte/s of traffic",
                    broker, currentUsage, overloadThreshold, minimumThroughputToOffload / 1024 / 1024);

            MutableDouble trafficMarkedToOffload = new MutableDouble(0);
            MutableBoolean atLeastOneBundleSelected = new MutableBoolean(false);

            if (localData.getBundles().size() > 1) {
                // Sort bundles by throughput, then pick the biggest N which combined make up for at least the minimum throughput to offload

                loadData.getBundleData().entrySet().stream().map((e) -> {
                    // Map to throughput value
                    // Consider short-term byte rate to address system resource burden
                    String bundle = e.getKey();
                    BundleData bundleData = e.getValue();
                    TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                    double throughput = shortTermData.getMsgThroughputIn() + shortTermData.getMsgThroughputOut();
                    return Pair.create(bundle, throughput);
                }).filter(e -> {
                    // Only consider bundles that were not already unloaded recently
                    return !recentlyUnloadedBundles.containsKey(e.first);
                }).sorted((e1, e2) -> {
                    // Sort by throughput in reverse order
                    return Double.compare(e2.second, e1.second);
                }).forEach(e -> {
                    if (trafficMarkedToOffload.doubleValue() < minimumThroughputToOffload
                            || atLeastOneBundleSelected.isFalse()) {
                       selectedBundlesCache.put(broker, e.first);
                       trafficMarkedToOffload.add(e.second);
                       atLeastOneBundleSelected.setTrue();
                   }
                });
            } else if (localData.getBundles().size() == 1) {
                log.warn(
                        "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. "
                                + "No Load Shedding will be done on this broker",
                        localData.getBundles().iterator().next(), broker);
            } else {
                log.warn("Broker {} is overloaded despite having no bundles", broker);
            }

        });

        return selectedBundlesCache;
    }
}
