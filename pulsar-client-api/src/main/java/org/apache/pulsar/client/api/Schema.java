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
package org.apache.pulsar.client.api;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Message schema definition.
 */
public interface Schema<T> extends Cloneable{

    /**
     * Check if the message is a valid object for this schema.
     *
     * <p>The implementation can choose what its most efficient approach to validate the schema.
     * If the implementation doesn't provide it, it will attempt to use {@link #decode(byte[])}
     * to see if this schema can decode this message or not as a validation mechanism to verify
     * the bytes.
     *
     * @param message the messages to verify
     * @throws SchemaSerializationException if it is not a valid message
     */
    default void validate(byte[] message) {
        decode(message);
    }

    /**
     * Encode an object representing the message content into a byte array.
     *
     * @param message
     *            the message object
     * @return a byte array with the serialized content
     * @throws SchemaSerializationException
     *             if the serialization fails
     */
    byte[] encode(T message);

    /**
     * Returns whether this schema supports versioning.
     *
     * <p>Most of the schema implementations don't really support schema versioning, or it just doesn't
     * make any sense to support schema versionings (e.g. primitive schemas). Only schema returns
     * {@link GenericRecord} should support schema versioning.
     *
     * <p>If a schema implementation returns <tt>false</tt>, it should implement {@link #decode(byte[])};
     * while a schema implementation returns <tt>true</tt>, it should implement {@link #decode(byte[], byte[])}
     * instead.
     *
     * @return true if this schema implementation supports schema versioning; otherwise returns false.
     */
    default boolean supportSchemaVersioning() {
        return false;
    }

    default void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
    }

    /**
     * Decode a byte array into an object using the schema definition and deserializer implementation.
     *
     * @param bytes
     *            the byte array to decode
     * @return the deserialized object
     */
    default T decode(byte[] bytes) {
        // use `null` to indicate ignoring schema version
        return decode(bytes, null);
    }

    /**
     * Decode a byte array into an object using a given version.
     *
     * @param bytes
     *            the byte array to decode
     * @param schemaVersion
     *            the schema version to decode the object. null indicates using latest version.
     * @return the deserialized object
     */
    default T decode(byte[] bytes, byte[] schemaVersion) {
        // ignore version by default (most of the primitive schema implementations ignore schema version)
        return decode(bytes);
    }

    /**
     * @return an object that represents the Schema associated metadata
     */
    SchemaInfo getSchemaInfo();

    /**
     * Check if this schema requires fetching schema info to configure the schema.
     *
     * @return true if the schema requires fetching schema info to configure the schema,
     *         otherwise false.
     */
    default boolean requireFetchingSchemaInfo() {
        return false;
    }

    /**
     * Configure the schema to use the provided schema info.
     *
     * @param topic topic name
     * @param componentName component name
     * @param schemaInfo schema info
     */
    default void configureSchemaInfo(String topic, String componentName,
                                     SchemaInfo schemaInfo) {
        // no-op
    }

    /**
     * Duplicates the schema.
     *
     * @return The duplicated schema.
     */
    Schema<T> clone();

    /**
     * Schema that doesn't perform any encoding on the message payloads. Accepts a byte array and it passes it through.
     */
    Schema<byte[]> BYTES = DefaultImplementation.newBytesSchema();

    /**
     * ByteBuffer Schema.
     */
    Schema<ByteBuffer> BYTEBUFFER = DefaultImplementation.newByteBufferSchema();

    /**
     * Schema that can be used to encode/decode messages whose values are String. The payload is encoded with UTF-8.
     */
    Schema<String> STRING = DefaultImplementation.newStringSchema();

    /**
     * INT8 Schema.
     */
    Schema<Byte> INT8 = DefaultImplementation.newByteSchema();

    /**
     * INT16 Schema.
     */
    Schema<Short> INT16 = DefaultImplementation.newShortSchema();

    /**
     * INT32 Schema.
     */
    Schema<Integer> INT32 = DefaultImplementation.newIntSchema();

    /**
     * INT64 Schema.
     */
    Schema<Long> INT64 = DefaultImplementation.newLongSchema();

    /**
     * Boolean Schema.
     */
    Schema<Boolean> BOOL = DefaultImplementation.newBooleanSchema();

    /**
     * Float Schema.
     */
    Schema<Float> FLOAT = DefaultImplementation.newFloatSchema();

    /**
     * Double Schema.
     */
    Schema<Double> DOUBLE = DefaultImplementation.newDoubleSchema();

    /**
     * Date Schema.
     */
    Schema<Date> DATE = DefaultImplementation.newDateSchema();

    /**
     * Time Schema.
     */
    Schema<Time> TIME = DefaultImplementation.newTimeSchema();

    /**
     * Timestamp Schema.
     */
    Schema<Timestamp> TIMESTAMP = DefaultImplementation.newTimestampSchema();

    // CHECKSTYLE.OFF: MethodName

    /**
     * Create a Protobuf schema type by extracting the fields of the specified class.
     *
     * @param clazz the Protobuf generated class to be used to extract the schema
     * @return a Schema instance
     */
    static <T extends com.google.protobuf.GeneratedMessageV3> Schema<T> PROTOBUF(Class<T> clazz) {
        return DefaultImplementation.newProtobufSchema(SchemaDefinition.builder().withPojo(clazz).build());
    }

    /**
     * Create a Protobuf schema type with schema definition.
     *
     * @param schemaDefinition schemaDefinition the definition of the schema
     * @return a Schema instance
     */
    static <T extends com.google.protobuf.GeneratedMessageV3> Schema<T> PROTOBUF(SchemaDefinition<T> schemaDefinition) {
        return DefaultImplementation.newProtobufSchema(schemaDefinition);
    }

    /**
     * Create a  Avro schema type by default configuration of the class.
     *
     * @param pojo the POJO class to be used to extract the Avro schema
     * @return a Schema instance
     */
    static <T> Schema<T> AVRO(Class<T> pojo) {
        return DefaultImplementation.newAvroSchema(SchemaDefinition.builder().withPojo(pojo).build());
    }

    /**
     * Create a Avro schema type with schema definition.
     *
     * @param schemaDefinition the definition of the schema
     * @return a Schema instance
     */
    static <T> Schema<T> AVRO(SchemaDefinition<T> schemaDefinition) {
        return DefaultImplementation.newAvroSchema(schemaDefinition);
    }

    /**
     * Create a JSON schema type by extracting the fields of the specified class.
     *
     * @param pojo the POJO class to be used to extract the JSON schema
     * @return a Schema instance
     */
    static <T> Schema<T> JSON(Class<T> pojo) {
        return DefaultImplementation.newJSONSchema(SchemaDefinition.builder().withPojo(pojo).build());
    }

    /**
     * Create a JSON schema type with schema definition.
     *
     * @param schemaDefinition the definition of the schema
     * @return a Schema instance
     */
    static <T> Schema<T> JSON(SchemaDefinition schemaDefinition) {
        return DefaultImplementation.newJSONSchema(schemaDefinition);
    }

    /**
     * Key Value Schema using passed in schema type, support JSON and AVRO currently.
     */
    static <K, V> Schema<KeyValue<K, V>> KeyValue(Class<K> key, Class<V> value, SchemaType type) {
        return DefaultImplementation.newKeyValueSchema(key, value, type);
    }

    /**
     * Schema that can be used to encode/decode KeyValue.
     */
    static Schema<KeyValue<byte[], byte[]>> KV_BYTES() {
        return DefaultImplementation.newKeyValueBytesSchema();
    }

    /**
     * Key Value Schema whose underneath key and value schemas are JSONSchema.
     */
    static <K, V> Schema<KeyValue<K, V>> KeyValue(Class<K> key, Class<V> value) {
        return DefaultImplementation.newKeyValueSchema(key, value, SchemaType.JSON);
    }

    /**
     * Key Value Schema using passed in key and value schemas.
     */
    static <K, V> Schema<KeyValue<K, V>> KeyValue(Schema<K> key, Schema<V> value) {
        return DefaultImplementation.newKeyValueSchema(key, value);
    }

    /**
     * Key Value Schema using passed in key, value and encoding type schemas.
     */
    static <K, V> Schema<KeyValue<K, V>> KeyValue(Schema<K> key, Schema<V> value,
        KeyValueEncodingType keyValueEncodingType) {
        return DefaultImplementation.newKeyValueSchema(key, value, keyValueEncodingType);
    }

    @Deprecated
    static Schema<GenericRecord> AUTO() {
        return AUTO_CONSUME();
    }

    /**
     * Create a schema instance that automatically deserialize messages
     * based on the current topic schema.
     *
     * <p>The messages values are deserialized into a {@link GenericRecord} object.
     *
     * <p>Currently this is only supported with Avro and JSON schema types.
     *
     * @return the auto schema instance
     */
    static Schema<GenericRecord> AUTO_CONSUME() {
        return DefaultImplementation.newAutoConsumeSchema();
    }

    /**
     * Create a schema instance that accepts a serialized payload
     * and validates it against the topic schema.
     *
     * <p>Currently this is only supported with Avro and JSON schema types.
     *
     * <p>This method can be used when publishing a raw JSON payload,
     * for which the format is known and a POJO class is not available.
     *
     * @return the auto schema instance
     */
    static Schema<byte[]> AUTO_PRODUCE_BYTES() {
        return DefaultImplementation.newAutoProduceSchema();
    }

    /**
     * Create a schema instance that accepts a serialized payload
     * and validates it against the schema specified.
     *
     * @return the auto schema instance
     * @since 2.5.0
     * @see #AUTO_PRODUCE_BYTES()
     */
    static Schema<byte[]> AUTO_PRODUCE_BYTES(Schema<?> schema) {
        return DefaultImplementation.newAutoProduceSchema(schema);
    }

    // CHECKSTYLE.ON: MethodName

    static Schema<?> getSchema(SchemaInfo schemaInfo) {
        return DefaultImplementation.getSchema(schemaInfo);
    }

    /**
     * Returns a generic schema of existing schema info.
     *
     * <p>Only supports AVRO and JSON.
     *
     * @param schemaInfo schema info
     * @return a generic schema instance
     */
    static GenericSchema<GenericRecord> generic(SchemaInfo schemaInfo) {
        return DefaultImplementation.getGenericSchema(schemaInfo);
    }

    static Schema getDefaultSchema(Class clazz, SchemaType defaultTypeIfNotMatch) {
        if (Byte[].class.equals(clazz)) {
            return Schema.BYTES;
        } else if (ByteBuffer.class.equals(clazz)) {
            return Schema.BYTEBUFFER;
        } else if (String.class.equals(clazz)) {
            return Schema.STRING;
        } else if (Byte.class.equals(clazz)) {
            return Schema.INT8;
        } else if (Short.class.equals(clazz)) {
            return Schema.INT16;
        } else if (Integer.class.equals(clazz)) {
            return Schema.INT32;
        } else if (Long.class.equals(clazz)) {
            return Schema.INT64;
        } else if (Boolean.class.equals(clazz)) {
            return Schema.BOOL;
        } else if (Float.class.equals(clazz)) {
            return Schema.FLOAT;
        } else if (Double.class.equals(clazz)) {
            return Schema.DOUBLE;
        } else if (Date.class.equals(clazz)) {
            return Schema.DATE;
        } else if (Time.class.equals(clazz)) {
            return Schema.TIME;
        } else if (Timestamp.class.equals(clazz)) {
            return Schema.TIMESTAMP;
        }
        if (defaultTypeIfNotMatch == SchemaType.JSON) {
            return Schema.JSON(clazz);
        }
        if (defaultTypeIfNotMatch == SchemaType.AVRO) {
            return Schema.AVRO(clazz);
        }
        if (defaultTypeIfNotMatch == SchemaType.PROTOBUF) {
            return Schema.PROTOBUF(clazz);
        }
        throw new IllegalArgumentException("Schema class type is incorrect");
    }

    static Schema getSchema(SchemaType schemaType, Class<?> ...pojo) {
        switch (schemaType) {
            case BYTES:
                return Schema.BYTES;
            case DATE:
                return Schema.DATE;
            case TIME:
                return Schema.TIME;
            case TIMESTAMP:
                return Schema.TIMESTAMP;
            case STRING:
                return Schema.STRING;
            case INT32:
                return Schema.INT32;
            case INT64:
                return Schema.INT64;
            case BOOLEAN:
                return Schema.BOOL;
            case FLOAT:
                return Schema.FLOAT;
            case DOUBLE:
                return Schema.DOUBLE;
            case INT8:
                return Schema.INT8;
            case INT16:
                return Schema.INT16;
            case AVRO:
                if (pojo == null || pojo.length < 1) {
                    throw new IllegalArgumentException("Schema avro param is incorrect");
                }
                return Schema.AVRO(pojo[0]);
            case JSON:
                if (pojo == null || pojo.length < 1) {
                    throw new IllegalArgumentException("Schema JSON param is incorrect");
                }
                return Schema.JSON(pojo[0]);
            case KEY_VALUE:
                if (pojo == null || pojo.length < 2) {
                    throw new IllegalArgumentException("Schema KEY_VALUE param is incorrect");
                }
                return Schema.KeyValue(pojo[0], pojo[1]);
            default:
                throw new IllegalArgumentException("Schema type type is incorrect");
        }
    }
}
