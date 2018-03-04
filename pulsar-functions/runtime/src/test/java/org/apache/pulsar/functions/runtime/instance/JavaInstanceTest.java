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
package org.apache.pulsar.functions.runtime.instance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.fs.LimitsConfig;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.api.utils.Utf8StringSerDe;
import org.apache.pulsar.functions.runtime.container.InstanceConfig;
import org.testng.annotations.Test;

import java.util.HashMap;

public class JavaInstanceTest {

    private class LongRunningHandler implements PulsarFunction<String, String> {
        @Override
        public String process(String input, Context context) throws Exception {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {

            }
            return input;
        }
    }

    private static InstanceConfig createInstanceConfig() {
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        LimitsConfig limitsConfig = new LimitsConfig();
        functionConfigBuilder.addInputs("TEST");
        functionConfigBuilder.setOutputSerdeClassName(Utf8StringSerDe.class.getName());
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionConfig(functionConfigBuilder.build());
        instanceConfig.setLimitsConfig(limitsConfig);
        return instanceConfig;
    }

    /**
     * Verify that functions running longer than time budget fails with Timeout exception
     * @throws Exception
     */
    @Test
    public void testLongRunningFunction() {
        InstanceConfig config = createInstanceConfig();
        config.getLimitsConfig().setMaxTimeMs(2000);
        JavaInstance instance = new JavaInstance(
            config, new LongRunningHandler(), null, null, new HashMap<>());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(MessageId.earliest, "random", testString);

        assertNull(result.getUserException());
        assertNotNull(result.getTimeoutException());
    }

    /**
     * Verify that be able to run lambda functions.
     * @throws Exception
     */
    @Test
    public void testLambda() {
        InstanceConfig config = createInstanceConfig();
        config.getLimitsConfig().setMaxTimeMs(2000);
        JavaInstance instance = new JavaInstance(
            config,
            (PulsarFunction<String, String>) (input, context) -> input + "-lambda",
            null, null, new HashMap<>());
        String testString = "ABC123";
        JavaExecutionResult result = instance.handleMessage(MessageId.earliest, "random", testString);
        assertNotNull(result.getResult());
        assertEquals(new String(testString + "-lambda"), result.getResult());
    }
}
