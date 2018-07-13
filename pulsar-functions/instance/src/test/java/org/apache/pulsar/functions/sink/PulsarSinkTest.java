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
package org.apache.pulsar.functions.sink;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

@Slf4j
public class PulsarSinkTest {

    private static final String TOPIC = "persistent://sample/standalone/ns1/test_result";
    private static final String serDeClassName = DefaultSerDe.class.getName();

    public static class TestSerDe implements SerDe<String> {

        @Override
        public String deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(String input) {
            return new byte[0];
        }
    }

    /**
     * Verify that JavaInstance does not support functions that take Void type as input
     */

    private static PulsarClient getPulsarClient() throws PulsarClientException {
        PulsarClient pulsarClient = mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = mock(ConsumerBuilder.class);
        doReturn(consumerBuilder).when(consumerBuilder).topics(anyList());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionName(anyString());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionType(any());
        doReturn(consumerBuilder).when(consumerBuilder).ackTimeout(anyLong(), any());
        Consumer consumer = mock(Consumer.class);
        doReturn(consumer).when(consumerBuilder).subscribe();
        doReturn(consumerBuilder).when(pulsarClient).newConsumer();
        return pulsarClient;
    }

    private static PulsarSinkConfig getPulsarConfigs() {
        PulsarSinkConfig pulsarConfig = new PulsarSinkConfig();
        pulsarConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        pulsarConfig.setTopic(TOPIC);
        pulsarConfig.setSerDeClassName(serDeClassName);
        pulsarConfig.setTypeClassName(String.class.getName());
        return pulsarConfig;
    }

    @Getter
    @Setter
    public static class ComplexUserDefinedType {
        private String name;
        private Integer age;
    }

    public static class ComplexSerDe implements SerDe<ComplexUserDefinedType> {
        @Override
        public ComplexUserDefinedType deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(ComplexUserDefinedType input) {
            return new byte[0];
        }
    }

    /**
     * Verify that JavaInstance does support functions that output Void type
     */
    @Test
    public void testVoidOutputClasses() throws Exception {
        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(Void.class.getName());
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new InstanceConfig());

        try {
            pulsarSink.setupSerDe();
        } catch (Exception ex) {
            ex.printStackTrace();
            assertEquals(ex, null);
            assertTrue(false);
        }
    }

    @Test
    public void testInconsistentOutputType() throws IOException {
        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to be inconsistent to that of SerDe
        pulsarConfig.setTypeClassName(Integer.class.getName());
        pulsarConfig.setSerDeClassName(TestSerDe.class.getName());
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new InstanceConfig());
        try {
            pulsarSink.setupSerDe();
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (RuntimeException ex) {
            log.error("RuntimeException: {}", ex, ex);
            assertTrue(ex.getMessage().startsWith("Inconsistent types found between function output type and output serde type:"));
        } catch (Exception ex) {
            log.error("Exception: {}", ex, ex);
            assertTrue(false);
        }
    }

    /**
     * Verify that Default Serializer works fine.
     */
    @Test
    public void testDefaultSerDe() throws PulsarClientException {

        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(String.class.getName());
        pulsarConfig.setSerDeClassName(null);
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new InstanceConfig());

        try {
            pulsarSink.setupSerDe();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
    }

    /**
     * Verify that Explicit setting of Default Serializer works fine.
     */
    @Test
    public void testExplicitDefaultSerDe() throws PulsarClientException {
        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(String.class.getName());
        pulsarConfig.setSerDeClassName(DefaultSerDe.class.getName());
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new InstanceConfig());

        try {
            pulsarSink.setupSerDe();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
    }

    @Test
    public void testComplexOuputType() throws PulsarClientException {
        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(ComplexUserDefinedType.class.getName());
        pulsarConfig.setSerDeClassName(ComplexSerDe.class.getName());
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new InstanceConfig());

        try {
            pulsarSink.setupSerDe();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
    }
}
