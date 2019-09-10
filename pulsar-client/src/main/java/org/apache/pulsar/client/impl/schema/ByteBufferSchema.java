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

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A bytebuffer schema is effectively a `BYTES` schema.
 */
public class ByteBufferSchema extends AbstractSchema<ByteBuffer> {

    public static ByteBufferSchema of() {
        return INSTANCE;
    }

    private static final ByteBufferSchema INSTANCE = new ByteBufferSchema();
    private static final SchemaInfo SCHEMA_INFO = new SchemaInfo()
        .setName("ByteBuffer")
        .setType(SchemaType.BYTES)
        .setSchema(new byte[0]);

    @Override
    public byte[] encode(ByteBuffer data) {
        if (data == null) {
            return null;
        }

        data.rewind();

        if (data.hasArray()) {
            byte[] arr = data.array();
            if (data.arrayOffset() == 0 && arr.length == data.remaining()) {
                return arr;
            }
        }

        byte[] ret = new byte[data.remaining()];
        data.get(ret, 0, ret.length);
        data.rewind();
        return ret;
    }

    @Override
    public ByteBuffer decode(byte[] data) {
        if (null == data) {
            return null;
        } else {
            return ByteBuffer.wrap(data);
        }
    }

    @Override
    public ByteBuffer decode(ByteBuf byteBuf) {
        if (null == byteBuf) {
            return null;
        } else {
            int size = byteBuf.readableBytes();
            return byteBuf.nioBuffer(0, byteBuf.readableBytes());
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
