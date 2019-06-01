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

import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;

import java.io.IOException;
import java.util.List;

/**
 * container for individual messages being published until they are batched and sent to broker
 */
public interface BatchMessageContainer {

    /**
     * Add message to the batch message container.
     *
     * @param msg message will add to the batch message container
     * @param callback message send callback
     */
    void add(MessageImpl<?> msg, SendCallback callback);

    /**
     * Check the batch message container have enough space for the message want to add.
     *
     * @param msg the message want to add
     * @return return true if the container have enough space for the specific message,
     *         otherwise return false.
     */
    boolean haveEnoughSpace(MessageImpl<?> msg);

    /**
     * Clear the message batch container.
     */
    void clear();

    /**
     * Check the message batch container is empty.
     *
     * @return return true if empty, otherwise return false.
     */
    boolean isEmpty();

    /**
     * Get count of messages in the message batch container.
     *
     * @return messages count
     */
    int getNumMessagesInBatch();

    /**
     * Get current message batch size of the message batch container in bytes.
     *
     * @return message batch size in bytes
     */
    long getCurrentBatchSizeBytes();

    /**
     * Set producer of the message batch container.
     *
     * @param producer producer
     */
    void setProducer(ProducerImpl<?> producer);

    /**
     * Release the payload and clear the container.
     *
     * @param ex cause
     */
    void handleException(Exception ex);

    /**
     * Create list of OpSendMsg, producer use OpSendMsg to send to the broker.
     *
     * @return list of OpSendMsg
     * @throws IOException
     */
    List<OpSendMsg> createOpSendMsgs() throws IOException;
}
