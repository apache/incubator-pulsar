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

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.sql.Time;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A schema for `java.sql.Time`.
 */
public class TimeSchema implements Schema<Time> {
   public static TimeSchema of() {
      return INSTANCE;
   }
   private static final org.apache.avro.Schema schema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);

   private static final TimeSchema INSTANCE = new TimeSchema();
   public static final SchemaInfo SCHEMA_INFO = new SchemaInfo()
         .setName("Time")
         .setType(SchemaType.TIME)
         .setSchema(schema.toString().getBytes(UTF_8));

   @Override
   public byte[] encode(Time message) {
      if (null == message) {
         return null;
      }

      Long time = message.getTime();
      return LongSchema.of().encode(time);
   }

   @Override
   public Time decode(byte[] bytes) {
      if (null == bytes) {
         return null;
      }

      Long decode = LongSchema.of().decode(bytes);
      return new Time(decode);
   }

   @Override
   public SchemaInfo getSchemaInfo() {
      return SCHEMA_INFO;
   }
}
