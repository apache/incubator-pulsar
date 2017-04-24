/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.api;

import java.io.IOException;

import com.yahoo.pulsar.client.impl.MessageIdImpl;

/**
 * Opaque unique identifier of a single message
 * 
 * The MessageId can be used to reference a specific message, for example when acknowledging, without having to retain
 * the message content in memory for an extended period of time.
 * 
 * 
 */
public interface MessageId {

    /**
     * Serialize the message ID into a byte array
     */
    byte[] toByteArray();

    /**
     * De-serialize a message id from a byte array
     * 
     * @param data
     *            byte array containing the serialized message id
     * @return the de-serialized messageId object
     */
    public static MessageId fromByteArray(byte[] data) throws IOException {
        return MessageIdImpl.fromByteArray(data);
    }
}
