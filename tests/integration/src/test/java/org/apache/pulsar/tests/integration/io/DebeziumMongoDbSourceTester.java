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
package org.apache.pulsar.tests.integration.io;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.DebeziumMongoDbContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

import java.io.Closeable;
import java.util.Map;

@Slf4j
public class DebeziumMongoDbSourceTester extends SourceTester<DebeziumMongoDbContainer> implements Closeable {

    private static final String NAME = "debezium-mongodb";

    private final String pulsarServiceUrl;

    @Getter
    private DebeziumMongoDbContainer debeziumMongoDbContainer;

    private final PulsarCluster pulsarCluster;
    public DebeziumMongoDbSourceTester(PulsarCluster cluster) {
        super(NAME);
        this.pulsarCluster = cluster;
        pulsarServiceUrl = "pulsar://pulsar-proxy:" + PulsarContainer.BROKER_PORT;

        sourceConfig.put("mongodb.hosts", "rs0/" + DebeziumMongoDbContainer.NAME + ":27107");
        sourceConfig.put("mongodb.name", "dbserver1");
        sourceConfig.put("mongodb.user", "debezium");
        sourceConfig.put("mongodb.password", "dbz");
        sourceConfig.put("mongodb.task.id","1");
        sourceConfig.put("database.whitelist", "inventory");
        sourceConfig.put("pulsar.service.url", pulsarServiceUrl);
    }

    @Override
    public void setServiceContainer(DebeziumMongoDbContainer container) {
        log.info("start debezium mongodb server container.");
        debeziumMongoDbContainer = container;
        pulsarCluster.startService(DebeziumMongoDbContainer.NAME, debeziumMongoDbContainer);
    }

    @Override
    public void prepareSource() throws Exception {
        this.debeziumMongoDbContainer.execCmd("bash","-c","/usr/local/bin/init-inventory.sh");
        log.info("debezium mongodb server already contains preconfigured data.");
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception {
        log.info("debezium mongodb server already contains preconfigured data.");
        return null;
    }

    @Override
    public String valueContains() {
        return "dbserver1.inventory.products.Envelope";
    }

    @Override
    public void close() {
        if (pulsarCluster != null) {
            pulsarCluster.stopService(DebeziumMongoDbContainer.NAME, debeziumMongoDbContainer);
        }
    }
}
