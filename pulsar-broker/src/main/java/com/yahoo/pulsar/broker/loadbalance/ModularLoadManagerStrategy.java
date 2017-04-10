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
package com.yahoo.pulsar.broker.loadbalance;

import java.util.Set;

import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate;

/**
 * Interface which serves as a component for ModularLoadManagerImpl, flexibly allowing the injection of potentially
 * complex strategies.
 */
public interface ModularLoadManagerStrategy {

    /**
     * Find a suitable broker to assign the given bundle to.
     * 
     * @param candidates
     *            The candidates for which the bundle may be assigned.
     * @param bundleToAssign
     *            The data for the bundle to assign.
     * @param loadData
     *            The load data from the leader broker.
     * @param conf
     *            The service configuration.
     * @return The name of the selected broker as it appears on ZooKeeper.
     */
    String selectBroker(Set<String> candidates, BundleData bundleToAssign, LoadData loadData,
            ServiceConfiguration conf);

    /**
     * Create a placement strategy using the configuration.
     * 
     * @param conf
     *            ServiceConfiguration to use.
     * @return A placement strategy from the given configurations.
     */
    static ModularLoadManagerStrategy create(final ServiceConfiguration conf) {
        try {
            // Only one strategy at the moment.
            return new LeastLongTermMessageRate(conf);
        } catch (Exception e) {
            // Ignore
        }
        return new LeastLongTermMessageRate(conf);
    }
}
