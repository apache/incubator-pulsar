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
package org.apache.pulsar.client.impl.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

public class ProtobufSchema<T extends com.google.protobuf.GeneratedMessageV3> implements Schema<T> {


    private Method fromBytes;
    private SchemaInfo schemaInfo;
    private ProtobufSchema(SchemaInfo schemaInfo, Class<T> pojo) {
        this.schemaInfo = schemaInfo;
        try {
            this.fromBytes = pojo.getMethod("parseFrom", byte[].class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] encode(T message) {
        return message.toByteArray();
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            return (T) this.fromBytes.invoke(null, bytes);
        } catch (Exception e) {
            throw new RuntimeException(new SchemaSerializationException(e));
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> ProtobufSchema<T> of(Class<T> pojo) {
        return of(pojo, Collections.emptyMap());
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> ProtobufSchema<T> of(
            Class<T> pojo, Map<String, String> properties){

        SchemaInfo info = new SchemaInfo();
        info.setName("");
        info.setProperties(properties);
        info.setType(SchemaType.PROTOBUF);

        //TODO determine best method to extract schema from a protobuf message
        info.setSchema(null);
        return new ProtobufSchema<>(info, pojo);
    }
}
