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

import java.util.concurrent.TimeUnit;

/**
 * Configuration for message batch receive {@link Consumer#batchReceive()} {@link Consumer#batchReceiveAsync()}.
 *
 * Batch receive policy can limit the number and bytes of messages in a single batch, and can specify a timeout
 * for waiting for enough messages for this batch.
 *
 * This batch receive will be completed as long as any one of the
 * conditions(has enough number of messages, has enough of size of messages, wait timeout) is met.
 *
 * Examples:
 *
 * 1.If set maxNumMessages = 10, maxSizeOfMessages = 1MB and without timeout, it
 * means {@link Consumer#batchReceive()} will always wait until there is enough messages.
 *
 * 2.If set maxNumberOfMessages = 0, maxNumBytes = 0 and timeout = 100ms, it
 * means {@link Consumer#batchReceive()} will waiting for 100ms whether or not there is enough messages.
 *
 * Note:
 * Must specify messages limitation(maxNumMessages, maxNumBytes) or wait timeout.
 * Otherwise, {@link Messages} ingest {@link Message} will never end.
 *
 * @since 2.4.1
 */
public class BatchReceivePolicy {

    /**
     * Default batch receive policy
     *
     * Max number of messages: 100
     * Max number of bytes: 10MB
     * Timeout: 100ms
     */
    public static final BatchReceivePolicy DEFAULT_POLICY = new BatchReceivePolicy(
            100, 1024 * 1024 * 10, 100, TimeUnit.MILLISECONDS);

    private BatchReceivePolicy(int maxNumMessages, long maxNumBytes, int timeout, TimeUnit timeoutUnit) {
        this.maxNumMessages = maxNumMessages;
        this.maxNumBytes = maxNumBytes;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    /**
     * Max number of messages for a single batch receive, 0 or negative means no limit.
     */
    private int maxNumMessages;

    /**
     * Max bytes of messages for a single batch receive, 0 or negative means no limit.
     */
    private long maxNumBytes;

    /**
     * timeout for waiting for enough messages(enough number or enough bytes).
     */
    private int timeout;
    private TimeUnit timeoutUnit;

    public void verify() {
        if (maxNumMessages <= 0 && maxNumBytes <= 0 && timeout <= 0) {
            throw new IllegalArgumentException("At least " +
                    "one of maxNumMessages, maxNumBytes, timeout must be specified.");
        }
        if (timeout > 0 && timeoutUnit == null) {
            throw new IllegalArgumentException("Must set timeout unit for timeout.");
        }
    }

    public long getTimeoutMs() {
        return (timeout > 0 && timeoutUnit != null) ? timeoutUnit.toMillis(timeout) : 0L;
    }

    public int getMaxNumMessages() {
        return maxNumMessages;
    }

    public long getMaxNumBytes() {
        return maxNumBytes;
    }

    public BatchReceivePolicy setMaxNumMessages(int maxNumMessages) {
        this.maxNumMessages = maxNumMessages;
        return this;
    }

    public BatchReceivePolicy setMaxNumBytes(long maxNumBytes) {
        this.maxNumBytes = maxNumBytes;
        return this;
    }

    private BatchReceivePolicy setTimeout(int timeout, TimeUnit timeoutUnit) {
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        return this;
    }

    public static class Builder {

        private int maxNumMessages;
        private long maxNumBytes;
        private int timeout;
        private TimeUnit timeoutUnit;

        public Builder maxNumMessages(int maxNumMessages) {
            this.maxNumMessages = maxNumMessages;
            return this;
        }

        public Builder maxNumBytes(long maxNumBytes) {
            this.maxNumBytes = maxNumBytes;
            return this;
        }

        public Builder timeout(int timeout, TimeUnit timeoutUnit) {
            this.timeout = timeout;
            this.timeoutUnit = timeoutUnit;
            return this;
        }

        public BatchReceivePolicy build() {
            return new BatchReceivePolicy(maxNumMessages, maxNumBytes, timeout, timeoutUnit);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "BatchReceivePolicy{" +
                "maxNumMessages=" + maxNumMessages +
                ", maxNumBytes=" + maxNumBytes +
                ", timeout=" + timeout +
                ", timeoutUnit=" + timeoutUnit +
                '}';
    }
}
