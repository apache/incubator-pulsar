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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import java.util.Objects;
import org.apache.pulsar.client.api.MessageId;

public class TopicMessageIdImpl implements MessageId {

    /** This topicName is get from ConsumerImpl, it contains partition part. */
    private final String topicName;
    private final MessageId messageId;

    TopicMessageIdImpl(String topicName, MessageId messageId) {
        this.topicName = topicName;
        this.messageId = messageId;
    }

    /**
     * Get the topic name without partition part of this message.
     * @return the name of the topic on which this message was published
     */
    public String getTopicName() {
        int position = topicName.lastIndexOf(PARTITIONED_TOPIC_SUFFIX);
        checkState(position != -1, "Topic Name not contains partition part. " + topicName);
        return topicName.substring(0, position);
    }

    /**
     * Get the topic name which contains partition part for this message.
     * @return the topic name which contains Partition part
     */
    public String getTopicPartitionName() {
        return topicName;
    }

    public MessageId getInnerMessageId() {
        return messageId;
    }

    @Override
    public byte[] toByteArray() {
        return messageId.toByteArray();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TopicMessageIdImpl)) {
            return false;
        }
        TopicMessageIdImpl other = (TopicMessageIdImpl) obj;
        return Objects.equals(topicName, other.topicName)
            && Objects.equals(messageId, other.messageId);
    }

    @Override
    public int compareTo(MessageId o) {
        return messageId.compareTo(o);
    }
}
