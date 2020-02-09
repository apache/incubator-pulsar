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
package org.apache.pulsar.transaction.coordinator.timeout;

import com.google.common.annotations.Beta;
import java.io.IOException;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;

/**
 * Factory of TransactionTimeoutTracker objects.
 */
@Beta
public interface TransactionTimeoutTrackerFactory extends AutoCloseable {
    /**
     * Initialize the factory implementation.
     */
    void initialize() throws IOException;

    /**
     * Create a new tracker instance.
     *
     * @param transactionMetadataStore
     *            a store for transaction metadata
     */
    TransactionTimeoutTracker newTracker(TransactionMetadataStore transactionMetadataStore);

    /**
     * Close the factory and release all the resources.
     */
    void close() throws IOException;
}
