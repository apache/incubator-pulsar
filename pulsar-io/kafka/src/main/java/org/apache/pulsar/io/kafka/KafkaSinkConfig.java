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

package org.apache.pulsar.io.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class KafkaSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        description =
            "A comma-separated list of host and port pairs that are the addresses of "
          + "the Kafka brokers that a Kafka client connects to initially bootstrap itself")
    private String bootstrapServers;
    @FieldDoc(
        required = true,
        defaultValue = "",
        description =
            "The number of acknowledgments the producer requires the leader to have received "
          + "before considering a request complete. This controls the durability of records that are sent.")
    private String acks;
    @FieldDoc(
        defaultValue = "16384L",
        description =
            "The batch size that Kafka producer will attempt to batch records together before sending them to brokers.")
    private long batchSize = 16384L;
    @FieldDoc(
        defaultValue = "1048576L",
        description =
            "The maximum size of a Kafka request in bytes.")
    private long maxRequestSize = 1048576L;
    @FieldDoc(
        required = true,
        defaultValue = "",
        description =
            "The Kafka topic that is used for Pulsar moving messages to.")
    private String topic;
    @FieldDoc(
        defaultValue = "org.apache.kafka.common.serialization.StringSerializer",
        description =
            "The serializer class for Kafka producer to serialize keys.")
    private String keySerializerClass = "org.apache.kafka.common.serialization.StringSerializer";
    @FieldDoc(
        defaultValue = "org.apache.kafka.common.serialization.ByteArraySerializer",
        description =
            "The serializer class for Kafka producer to serialize values. You typically shouldn't care this. "
          + "Since the serializer will be set by a specific implementation of `KafkaAbstractSink`.")
    private String valueSerializerClass = "org.apache.kafka.common.serialization.ByteArraySerializer";
    @FieldDoc(
        defaultValue = "",
        description =
            "The producer config properties to be passed to Producer. Note that other properties specified "
          + "in the connector config file take precedence over this config.")
    private Map<String, Object> producerConfigProperties;

    public static KafkaSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), KafkaSinkConfig.class);
    }

    public static KafkaSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), KafkaSinkConfig.class);
    }
}