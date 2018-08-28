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

package org.apache.pulsar.io.jdbc;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.NotNull;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Jdbc Sink test
 */
@Slf4j
public class JdbcSinkTest {
    private final SqliteUtils sqliteUtils = new SqliteUtils(getClass().getSimpleName());
    private JdbcAvroSchemaSink jdbcSink;
    Map<String, Object> conf;
    String tableName = "TestJdbcFooSink";

    /**
     * A Simple class to test jdbc class
     */
    @Data
    @ToString
    @EqualsAndHashCode
    public static class Foo {
        @NotNull
        private final String field1;
        @NotNull
        private final String field2;
        @NotNull
        private final int field3;
    }

    @BeforeMethod
    public void setUp() throws Exception {
        sqliteUtils.setUp();

        String jdbcUrl = sqliteUtils.sqliteUri();
        conf = Maps.newHashMap();
        conf.put("jdbcUrl", jdbcUrl);
        conf.put("tableName", tableName);

        jdbcSink = new JdbcAvroSchemaSink();

        sqliteUtils.createTable(
            "CREATE TABLE " + tableName + "(" +
                "    field1  TEXT," +
                "    field2  TEXT," +
                "    field3 INTEGER," +
                "PRIMARY KEY (field1));"
        );
    }

    @AfterMethod
    public void tearDown() throws Exception {
        jdbcSink.close();
        sqliteUtils.tearDown();
    }

    @Test
    public void TestOpenAndWriteSink() throws Exception {
        // prepare a foo Record
        Foo obj = new Foo("ValueOfField1", "ValueOfField1", 3);
        AvroSchema<Foo> schema = AvroSchema.of(Foo.class);
        conf.put("schema",  new String(schema.getSchemaInfo().getSchema()));
        log.info("schema: {}", new String(schema.getSchemaInfo().getSchema()));

        byte[] bytes = schema.encode(obj);
        ByteBuf payload = Unpooled.copiedBuffer(bytes);
        Message<byte[]> message = new MessageImpl("77:777", conf, payload, Schema.BYTES);
        Record<byte[]> record = PulsarRecord.<byte[]>builder()
            .message(message)
            .topicName("fake_topic_name")
            .build();

        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
            obj.toString(),
            message.getValue().toString(),
            record.getValue().toString());

        // change batchSize to 1, to flush each write.
        conf.put("batchSize", 1);
        // open should success
        jdbcSink.open(conf, null);

        // write should success.
        jdbcSink.write(record);
        log.info("executed write");

        // value has been written to db, read it out and verify.
        String querySql = "SELECT * FROM " + tableName;
        sqliteUtils.select(querySql, (resultSet) -> {
            Assert.assertEquals(obj.getField1(), resultSet.getString(1));
            Assert.assertEquals(obj.getField2(), resultSet.getString(2));
            Assert.assertEquals(obj.getField3(), resultSet.getInt(3));
        });
    }
}
