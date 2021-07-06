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
package org.apache.pulsar.broker.service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.RateLimitFunction;
import org.apache.pulsar.common.util.RateLimiter;

public class PrecisPublishLimiter implements PublishRateLimiter, AutoCloseable {
    protected volatile int publishMaxMessageRate = 0;
    protected volatile long publishMaxByteRate = 0;
    protected volatile boolean publishThrottlingEnabled = false;
    // precise mode for publish rate limiter
    private volatile RateLimiter topicPublishRateLimiterOnMessage;
    private volatile RateLimiter topicPublishRateLimiterOnByte;
    private final RateLimitFunction rateLimitFunction;
    private final ScheduledExecutorService scheduledExecutorService;

    public PrecisPublishLimiter(Policies policies, String clusterName, RateLimitFunction rateLimitFunction) {
        this.rateLimitFunction = rateLimitFunction;
        update(policies, clusterName);
        this.scheduledExecutorService = null;
    }

    public PrecisPublishLimiter(PublishRate publishRate, RateLimitFunction rateLimitFunction) {
        this(publishRate, rateLimitFunction, null);
    }

    public PrecisPublishLimiter(PublishRate publishRate, RateLimitFunction rateLimitFunction,
                                ScheduledExecutorService scheduledExecutorService) {
        this.rateLimitFunction = rateLimitFunction;
        update(publishRate);
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    public void checkPublishRate() {
        // No-op
    }

    @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
        // No-op
    }

    @Override
    public boolean resetPublishCount() {
        return true;
    }

    @Override
    public boolean isPublishRateExceeded() {
        return false;
    }


    @Override
    public void update(Policies policies, String clusterName) {
        final PublishRate maxPublishRate = policies.publishMaxMessageRate != null
                ? policies.publishMaxMessageRate.get(clusterName)
                : null;
        this.update(maxPublishRate);
    }

    public void update(PublishRate maxPublishRate) {
        replaceLimiters(() -> {
            if (maxPublishRate != null
                    && (maxPublishRate.publishThrottlingRateInMsg > 0
                    || maxPublishRate.publishThrottlingRateInByte > 0)) {
                this.publishThrottlingEnabled = true;
                this.publishMaxMessageRate = Math.max(maxPublishRate.publishThrottlingRateInMsg, 0);
                this.publishMaxByteRate = Math.max(maxPublishRate.publishThrottlingRateInByte, 0);
                if (this.publishMaxMessageRate > 0) {
                    topicPublishRateLimiterOnMessage =
                            new RateLimiter(scheduledExecutorService, publishMaxMessageRate, 1,
                                    TimeUnit.SECONDS, rateLimitFunction);
                }
                if (this.publishMaxByteRate > 0) {
                    // TODO: is it intentional that rateLimitFunction isn't passed to topicPublishRateLimiterOnByte?
                    topicPublishRateLimiterOnByte = new RateLimiter(scheduledExecutorService, publishMaxByteRate,
                            1, TimeUnit.SECONDS);
                }
            } else {
                this.publishMaxMessageRate = 0;
                this.publishMaxByteRate = 0;
                this.publishThrottlingEnabled = false;
            }
        });
    }

    @Override
    public boolean tryAcquire(int numbers, long bytes) {
        return (topicPublishRateLimiterOnMessage == null || topicPublishRateLimiterOnMessage.tryAcquire(numbers))
                && (topicPublishRateLimiterOnByte == null || topicPublishRateLimiterOnByte.tryAcquire(bytes));
    }

    @Override
    public void close() throws Exception {
        replaceLimiters(null);
    }

    private void replaceLimiters(Runnable updater) {
        RateLimiter previousTopicPublishRateLimiterOnMessage = topicPublishRateLimiterOnMessage;
        topicPublishRateLimiterOnMessage = null;
        RateLimiter previousTopicPublishRateLimiterOnByte = topicPublishRateLimiterOnByte;
        topicPublishRateLimiterOnByte = null;
        try {
            if (updater != null) {
                updater.run();
            }
        } finally {
            // Close previous limiters to prevent resource leakages.
            // Delay closing of previous limiters after new ones are in place so that updating the limiter
            // doesn't cause unavailability.
            if (previousTopicPublishRateLimiterOnMessage != null) {
                previousTopicPublishRateLimiterOnMessage.close();
            }
            if (previousTopicPublishRateLimiterOnByte != null) {
                previousTopicPublishRateLimiterOnByte.close();
            }
        }
    }
}
