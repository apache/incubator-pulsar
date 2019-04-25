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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.schema.KeyValue;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Unit test of {@link MessageImpl}.
 */
public class MessageImplTest {

    @Test
    public void testGetSequenceIdNotAssociated() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertEquals(-1, msg.getSequenceId());
    }

    @Test
    public void testGetSequenceIdAssociated() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
            .setSequenceId(1234);

        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertEquals(1234, msg.getSequenceId());
    }

    @Test
    public void testGetProducerNameNotAssigned() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertNull(msg.getProducerName());
    }

    @Test
    public void testGetProducerNameAssigned() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
            .setProducerName("test-producer");

        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertEquals("test-producer", msg.getProducerName());
    }

    @Test
    public void testGetProducerDataAssigned() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("test-producer");

//        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(fooSchema, barSchema);
        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);
        Map<String, String> properties = Maps.newHashMap();

        // // Check kv.encoding.type default, not set value
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);

        // Check kv.encoding.type INLINE
        properties.put("kv.encoding.type", "INLINE");
        keyValueSchema.getSchemaInfo().setProperties(properties);
        encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        msg = MessageImpl.create(builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);

        // Check kv.encoding.type SPRAERATE
        properties.put("kv.encoding.type", "SEPARATED");
        keyValueSchema.getSchemaInfo().setProperties(properties);
        encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        msg = MessageImpl.create(builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        builder.setPartitionKey(new String(fooSchema.encode(foo)));
        keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
    }
}
