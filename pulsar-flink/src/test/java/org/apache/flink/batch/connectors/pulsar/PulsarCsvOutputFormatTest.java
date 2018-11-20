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
package org.apache.flink.batch.connectors.pulsar;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 * Tests for PulsarCsvOutputFormat
 */
public class PulsarCsvOutputFormatTest {

    private List<String> fieldNames = Arrays.asList("test-field");

    @Test(expected = NullPointerException.class)
    public void testPulsarCsvOutputFormatConstructorWhenServiceUrlIsNull() {
        new PulsarCsvOutputFormat(null, "testTopic", fieldNames);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarCsvOutputFormatConstructorWhenTopicNameIsNull() {
        new PulsarCsvOutputFormat("testServiceUrl", null, fieldNames);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarCsvOutputFormatConstructorWhenTopicNameIsBlank() {
        new PulsarCsvOutputFormat("testServiceUrl", " ", fieldNames);
    }

    @Test(expected = NullPointerException.class)
    public void testPulsarCsvOutputFormatConstructorWhenFieldNamesPropertyIsNull() {
        new PulsarCsvOutputFormat("testServiceUrl", "testTopic", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarCsvOutputFormatConstructorWhenFieldNamesPropertyIsEmpty() {
        new PulsarCsvOutputFormat("testServiceUrl", "testTopic", Arrays.asList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarCsvOutputFormatConstructorWhenFieldNamesPropertyHasBlankFieldName() {
        new PulsarCsvOutputFormat("testServiceUrl", "testTopic", Arrays.asList("test-field", " "));
    }

    @Test
    public void testPulsarCsvOutputFormatConstructor() {
        PulsarCsvOutputFormat pulsarCsvOutputFormat =
                new PulsarCsvOutputFormat("testServiceUrl", "testTopic", fieldNames);
        assertNotNull(pulsarCsvOutputFormat);
    }
}
