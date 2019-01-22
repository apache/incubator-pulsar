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
package org.apache.pulsar.io.core;

import org.slf4j.Logger;

import java.nio.ByteBuffer;

public interface SourceContext {

    /**
     * The id of the instance that invokes this source.
     *
     * @return the instance id
     */
    int getInstanceId();

    /**
     * Get the number of instances that invoke this source.
     *
     * @return the number of instances that invoke this source.
     */
    int getNumInstances();

    /**
     * Record a user defined metric
     * @param metricName The name of the metric
     * @param value The value of the metric
     */
    void recordMetric(String metricName, double value);

    /**
     * Get the output topic of the source
     * @return output topic name
     */
    String getOutputTopic();

    /**
     * The tenant this source belongs to
     * @return the tenant this source belongs to
     */
    String getTenant();

    /**
     * The namespace this source belongs to
     * @return the namespace this source belongs to
     */
    String getNamespace();

    /**
     * The name of the source that we are executing
     * @return The Source name
     */
    String getSourceName();

    /**
     * The logger object that can be used to log in a source
     * @return the logger object
     */
    Logger getLogger();

    /**
     * Increment the builtin distributed counter refered by key
     * @param key The name of the key
     * @param amount The amount to be incremented
     */
    void incrCounter(String key, long amount);

    /**
     * Retrieve the counter value for the key.
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    long getCounter(String key);

    /**
     * Updare the state value for the key.
     *
     * @param key name of the key
     * @param value state value of the key
     */
    void putState(String key, ByteBuffer value);

    /**
     * Retrieve the state value for the key.
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    ByteBuffer getState(String key);
}
