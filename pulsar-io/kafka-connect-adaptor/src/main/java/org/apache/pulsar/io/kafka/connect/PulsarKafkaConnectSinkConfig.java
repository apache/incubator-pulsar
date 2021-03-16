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

package org.apache.pulsar.io.kafka.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class PulsarKafkaConnectSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
            defaultValue = "1",
            help = "The batch size that Kafka producer will attempt to batch records together.")
    private int batchSize = 1;

    @FieldDoc(
            defaultValue = "2147483647L",
            help = "The batch size that Kafka producer will attempt to batch records together.")
    private long lingerTimeMs = 2147483647L;

    @FieldDoc(
            defaultValue = "pulsar-io-adaptor-topic",
            help = "The Kafka topic name that passed to kafka sink.")
    private String topic = "pulsar-io-adaptor-topic";

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "A kafka-connector sink class to use.")
    private String kafkaConnectorSinkClass;

    @FieldDoc(
            defaultValue = "",
            help = "Config properties to pass to the kafka connector.")
    private Map<String, String> kafkaConnectorConfigProperties;

    @FieldDoc(
            defaultValue = "STRING_SCHEMA",
            help = "Default Kafka Connect Key Schema to use if record does not specify one.")
    private String defaultKeySchema = "STRING_SCHEMA";

    @FieldDoc(
            defaultValue = "BYTES_SCHEMA",
            help = "Default Kafka Connect Value Schema to use if record does not specify one.")
    private String defaultValueSchema = "BYTES_SCHEMA";

    @FieldDoc(
            defaultValue = "kafka-adaptor-sink-offsets",
            help = "Pulsar topic to store offsets at.")
    private String offsetStorageTopic = "kafka-adaptor-sink-offsets";

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "Pulsar service URL.")
    private String pulsarServiceUrl;

    @FieldDoc(
            defaultValue = "true",
            help = "In case of Record<KeyValue<>> data use key from KeyValue<> instead of one from Record.")
    private boolean unwrapKeyValueIfAvailable = true;

    public static PulsarKafkaConnectSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), PulsarKafkaConnectSinkConfig.class);
    }

    public static PulsarKafkaConnectSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), PulsarKafkaConnectSinkConfig.class);
    }
}