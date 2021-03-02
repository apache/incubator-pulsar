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

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;

import org.apache.pulsar.client.api.schema.*;

import java.nio.ByteBuffer;


/**
 * This is a wrapper around a Byte array (the Avro encoded record) and a schema id in the Kafka Schema Registry.
 */
@Data
public class BytesWithSchema {
    private final byte[] value;
    private final int schemaId;
}
