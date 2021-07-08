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
package org.apache.pulsar.broker.resources;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;

public class BookieResources extends BaseResources<BookiesRackConfiguration> {

    public BookieResources(MetadataStoreExtended store, int operationTimeoutSec) {
        super(store, BookiesRackConfiguration.class, operationTimeoutSec);
    }

    public CompletableFuture<Optional<BookiesRackConfiguration>> get() {
        return getAsync(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH);
    }

    public CompletableFuture<Void> update(Function<Optional<BookiesRackConfiguration>,
            BookiesRackConfiguration> modifyFunction) {
        return getCache().readModifyUpdateOrCreate(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                modifyFunction).thenApply(__ -> null);
    }
}
