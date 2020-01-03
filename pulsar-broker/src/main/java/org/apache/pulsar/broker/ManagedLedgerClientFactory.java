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
package org.apache.pulsar.broker;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.EnsemblePlacementPolicyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

public class ManagedLedgerClientFactory implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerClientFactory.class);

    private final ManagedLedgerFactory managedLedgerFactory;
    private final BookKeeper defaultBkClient;
    private final Map<EnsemblePlacementPolicyConfig, BookKeeper> bkEnsemblePolicyToBkClientMap = Maps.newConcurrentMap();

    public ManagedLedgerClientFactory(BookKeeperClientFactory bookkeeperProvider, PulsarService pulsar) throws Exception {
        ServiceConfiguration conf = pulsar.getConfiguration();
        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        managedLedgerFactoryConfig.setMaxCacheSize(conf.getManagedLedgerCacheSizeMB() * 1024L * 1024L);
        managedLedgerFactoryConfig.setCacheEvictionWatermark(conf.getManagedLedgerCacheEvictionWatermark());
        managedLedgerFactoryConfig.setNumManagedLedgerWorkerThreads(conf.getManagedLedgerNumWorkerThreads());
        managedLedgerFactoryConfig.setNumManagedLedgerSchedulerThreads(conf.getManagedLedgerNumSchedulerThreads());
        managedLedgerFactoryConfig.setCacheEvictionFrequency(conf.getManagedLedgerCacheEvictionFrequency());
        managedLedgerFactoryConfig.setCacheEvictionTimeThresholdMillis(conf.getManagedLedgerCacheEvictionTimeThresholdMillis());
        managedLedgerFactoryConfig.setThresholdBackloggedCursor(conf.getManagedLedgerCursorBackloggedThreshold());
        managedLedgerFactoryConfig.setCopyEntriesInCache(conf.isManagedLedgerCacheCopyEntries());

        this.defaultBkClient = bookkeeperProvider.create(pulsar, Optional.empty(), null);

        BookkeeperFactoryForCustomEnsemblePlacementPolicy bkFactory = (
            EnsemblePlacementPolicyConfig ensemblePlacementPolicyConfig) -> {
            BookKeeper bkClient = null;
            // find or create bk-client in cache for a specific ensemblePlacementPolicy
            if (ensemblePlacementPolicyConfig != null && ensemblePlacementPolicyConfig.getPolicyClass() != null) {
                bkClient = bkEnsemblePolicyToBkClientMap.computeIfAbsent(ensemblePlacementPolicyConfig, (key) -> {
                    try {
                        return bookkeeperProvider.create(pulsar,
                            Optional.ofNullable(ensemblePlacementPolicyConfig.getPolicyClass()),
                            ensemblePlacementPolicyConfig.getProperties());
                    } catch (Exception e) {
                        log.error("Failed to initialize bk-client for policy {}, properties {}",
                            ensemblePlacementPolicyConfig.getPolicyClass(),
                            ensemblePlacementPolicyConfig.getProperties(), e);
                    }
                    return this.defaultBkClient;
                });
            }
            return bkClient != null ? bkClient : defaultBkClient;
        };

        this.managedLedgerFactory = new ManagedLedgerFactoryImpl(bkFactory, pulsar.getZkClient(), managedLedgerFactoryConfig);
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerFactory;
    }

    public BookKeeper getBookKeeperClient() {
        return defaultBkClient;
    }

    @VisibleForTesting
    public Map<EnsemblePlacementPolicyConfig, BookKeeper> getBkEnsemblePolicyToBookKeeperMap() {
        return bkEnsemblePolicyToBkClientMap;
    }

    public void close() throws IOException {
        try {
            managedLedgerFactory.shutdown();
            log.info("Closed managed ledger factory");

            try {
                defaultBkClient.close();
            } catch (RejectedExecutionException ree) {
                // when closing bookkeeper client, it will error outs all pending metadata operations.
                // those callbacks of those operations will be triggered, and submitted to the scheduler
                // in managed ledger factory. but the managed ledger factory has been shutdown before,
                // so `RejectedExecutionException` will be thrown there. we can safely ignore this exception.
                //
                // an alternative solution is to close bookkeeper client before shutting down managed ledger
                // factory, however that might be introducing more unknowns.
                log.warn("Encountered exceptions on closing bookkeeper client", ree);
            }
            if (bkEnsemblePolicyToBkClientMap != null) {
                bkEnsemblePolicyToBkClientMap.forEach((policy, bk) -> {
                    try {
                        if (bk != null) {
                            bk.close();
                        }
                    } catch (Exception e) {
                        log.warn("Failed to close bookkeeper-client for policy {}", policy, e);
                    }
                });
            }
            log.info("Closed BookKeeper client");
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            throw new IOException(e);
        }
    }
}
