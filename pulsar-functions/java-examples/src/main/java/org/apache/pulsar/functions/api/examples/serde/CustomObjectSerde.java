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
package org.apache.pulsar.functions.api.examples.serde;

import org.apache.pulsar.functions.api.SerDe;

import java.nio.ByteBuffer;

/**
 * This class takes care of serializing/deserializing CustomObject.
 */
public class CustomObjectSerde implements SerDe<CustomObject> {
    @Override
    public CustomObject deserialize(byte[] bytes) {
        ByteBuffer buffer =  ByteBuffer.wrap(bytes);
        return new CustomObject(buffer.getLong());
    }

    @Override
    public byte[] serialize(CustomObject object) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(object.getValue());
        return buffer.array();
    }
}
