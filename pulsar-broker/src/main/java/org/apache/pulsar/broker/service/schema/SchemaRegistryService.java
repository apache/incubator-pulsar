/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.schema;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.schema.SchemaRegistry;
import org.apache.pulsar.common.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SchemaRegistryService extends SchemaRegistry {
    String CreateMethodName = "create";
    Logger log = LoggerFactory.getLogger(SchemaRegistryService.class);

    static SchemaRegistryService create(PulsarService pulsar) {
        try {
            ServiceConfiguration config = pulsar.getConfiguration();
            final Class<?> storageClass = Class.forName(config.getSchemaRegistryStorageClassName());
            Object factoryInstance = storageClass.newInstance();
            Method createMethod = storageClass.getMethod(CreateMethodName, PulsarService.class);
            SchemaStorage schemaStorage = (SchemaStorage) createMethod.invoke(factoryInstance, pulsar);
            return new SchemaRegistryServiceImpl(schemaStorage);
        } catch (Exception e) {
            log.warn("Error when trying to create scehema registry storage: {}", e);
        }
        return new DefaultSchemaRegistryService();
    }

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId);

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, long version);

    CompletableFuture<Long> putSchema(String schemaId, Schema schema);

    CompletableFuture<Long> deleteSchema(String schemaId, String user);

    void close() throws Exception;
}
