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
package org.apache.pulsar.transaction.coordinator.impl;

import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The provider that offers managed ledger implementation of {@link TransactionMetadataStore}.
 */
public class ManagedLedgerTransactionMetadataStoreProvider implements TransactionMetadataStoreProvider {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTransactionMetadataStoreProvider.class);
    @Override
    public CompletableFuture<TransactionMetadataStore>
    openStore(TransactionCoordinatorID transactionCoordinatorId, ManagedLedgerFactory managedLedgerFactory) {
        TransactionMetadataStore transactionMetadataStore;
        try {
            transactionMetadataStore =
                    new ManagedLedgerTransactionMetadataStore(transactionCoordinatorId, managedLedgerFactory);
        } catch (Exception e) {
            log.error("ManagedLedgerTransactionMetadataStore init fail", e);
            return FutureUtil.failedFuture(e);
        }
        return CompletableFuture.completedFuture(transactionMetadataStore);
    }
}
