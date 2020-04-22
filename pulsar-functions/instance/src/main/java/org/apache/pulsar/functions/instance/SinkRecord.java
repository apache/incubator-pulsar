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
package org.apache.pulsar.functions.instance;

import java.util.Map;
import java.util.Optional;

import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;

@Slf4j
@Data
@AllArgsConstructor
public class SinkRecord<T> implements Record<T> {

    private final Record<T> sourceRecord;
    private final T value;

    public Record<T> getSourceRecord() {
        return sourceRecord;
    }

    @Override
    public Optional<String> getTopicName() {
        return sourceRecord.getTopicName();
    }

    @Override
    public Optional<String> getKey() {
        return sourceRecord.getKey();
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public Optional<String> getPartitionId() {
        return sourceRecord.getPartitionId();
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return sourceRecord.getRecordSequence();
    }

     @Override
    public Map<String, String> getProperties() {
        return sourceRecord.getProperties();
    }

    @Override
    public void ack() {
        sourceRecord.ack();
    }

    @Override
    public void fail() {
        sourceRecord.fail();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return sourceRecord.getDestinationTopic();
    }

    @Override
    public Schema<T> getSchema() {
        if (sourceRecord == null) {
            return null;
        }

        if (sourceRecord.getSchema() != null) {
            return sourceRecord.getSchema();
        }

        log.info("[SinkRecord] Schema classLoader: {}", Schema.class.getClassLoader());
        log.info("[SinkRecord] sourceRecord classLoader: {}", sourceRecord.getClass().getClassLoader());
        log.info("[SinkRecord] KVRecord classLoader: {}", KVRecord.class.getClassLoader());

        if (sourceRecord instanceof KVRecord) {
            KVRecord kvRecord = (KVRecord) sourceRecord;
            log.info("[SinkRecord] keySchema classLoader: {}, schemaInfo: {}",
                    kvRecord.getKeySchema().getClass().getClassLoader(), kvRecord.getKeySchema().getSchemaInfo().toString());
            log.info("[SinkRecord] valueSchema classLoader: {}, schemaInfo: {}",
                    kvRecord.getValueSchema().getClass().getClassLoader(), kvRecord.getValueSchema().getSchemaInfo().toString());
            return KeyValueSchema.of(kvRecord.getKeySchema(), kvRecord.getValueSchema(),
                    kvRecord.getKeyValueEncodingType());
        }

        return null;

//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//        try {
//            ObjectOutputStream oos = new ObjectOutputStream(byteArrayOutputStream);
//            oos.writeObject(sourceRecord.getSchema());
//            oos.flush();
//            oos.close();
//
//            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
//            ObjectInputStream ois = new ObjectInputStream(byteArrayInputStream);
//            Schema schema = (Schema) ois.readObject();
//            log.info("deserializable schema: {}, classLoader: {}",
//                    schema.getClass().getName(), schema.getClass().getClassLoader());
//            return schema;
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//            return null;
//        }

//        log.info("[SinkRecord] Schema classLoader: {}", Schema.class.getClassLoader());
//        if (sourceRecord != null && sourceRecord.getSchema() != null) {
//            SchemaInfo srcSchemaInfo = sourceRecord.getSchema().getSchemaInfo();
//            log.info("[SinkRecord] map classLoader: {}", srcSchemaInfo.getProperties().getClass().getClassLoader());
//            SchemaInfo schemaInfo = SchemaInfo.builder()
//                    .name(srcSchemaInfo.getName())
//                    .schema(srcSchemaInfo.getSchema())
//                    .type(SchemaType.valueOf(srcSchemaInfo.getType().getValue()))
//                    .properties(srcSchemaInfo.getProperties())
//                    .build();
//            Schema<T> schema = (Schema<T>) Schema.getSchema(schemaInfo);
//            log.info("[SinkRecord] schemaInfo: {}", schemaInfo);
//            return schema;
//        } else {
//            return null;
//        }
    }
}
