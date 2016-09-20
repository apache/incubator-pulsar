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
package com.yahoo.pulsar.broker.service;

import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static com.yahoo.pulsar.broker.ServiceConfigurationLoader.create;

public class AdvertisedAddressTest {

    LocalBookkeeperEnsemble bkEnsemble;
    PulsarService pulsar;

    private final int ZOOKEEPER_PORT = 12759;
    private final int BROKER_WEBSERVICE_PORT = 15782;
    private final int BROKER_SERVICE_PORT = 16650;

    private final String bindAddress = "127.0.0.1";
    private final String advertisedAddress = "pulsar-usc.example.com";

    @Before
    public void setup() throws Exception {
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, 5001);
        bkEnsemble.start();
        ServiceConfiguration config = create(new Properties(System.getProperties()));
        config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config.setWebServicePort(BROKER_WEBSERVICE_PORT);
        config.setClusterName("usc");
        config.setBrokerServicePort(BROKER_SERVICE_PORT);
        config.setBindAddress(bindAddress);
        config.setAdvertisedAddress(advertisedAddress);
        config.setManagedLedgerMaxEntriesPerLedger(5);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        pulsar = new PulsarService(config);
        pulsar.start();
    }

    @After
    public void shutdown() throws Exception {
        pulsar.close();
        bkEnsemble.stop();
    }

    @Test
    public void testAdvertisedAddress() throws Exception {
        Assert.assertEquals( pulsar.getHost(), bindAddress );
        Assert.assertEquals( pulsar.getAdvertisedAddress(), advertisedAddress );
        Assert.assertEquals( pulsar.getBrokerServiceUrl(), String.format("pulsar://%s:%d", advertisedAddress, BROKER_SERVICE_PORT) );
        Assert.assertEquals( pulsar.getWebServiceAddress(), String.format("http://%s:%d", advertisedAddress, BROKER_WEBSERVICE_PORT) );
        String brokerZkPath = String.format("/loadbalance/brokers/%s:%d", bindAddress, BROKER_WEBSERVICE_PORT);
        String bkBrokerData = new String(bkEnsemble.getZkClient().getData(brokerZkPath, false, new Stat()), StandardCharsets.UTF_8);
        JSONObject jsonBkBrokerData = new JSONObject(bkBrokerData);
        Assert.assertEquals( jsonBkBrokerData.get("pulsarServiceUrl"), pulsar.getBrokerServiceUrl() );
        Assert.assertEquals( jsonBkBrokerData.get("webServiceUrl"), pulsar.getWebServiceAddress() );
    }

}
