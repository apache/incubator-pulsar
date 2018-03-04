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
package org.apache.pulsar.functions.composition.windowing;

/**
 * An event is a wrapper object that gets stored in the window.
 *
 * @param <T> the type of the object thats wrapped
 */
public interface Event<T> {
    /**
     * The event timestamp in millis
     *
     * @return the event timestamp in milliseconds.
     */
    long getTimestamp();

    /**
     * Returns the wrapped object
     *
     * @return the wrapped object.
     */
    T get();

    /**
     * If this is a watermark event or not. Watermark events are used
     * for tracking time while processing event based ts.
     *
     * @return true if this is a watermark event
     */
    boolean isWatermark();


    /**
     * Get the message id of this event
     *
     * @return byte array of the message id
     */
    byte[] getMessageId();

    /**
     * Get the topic this event was sent from
     *
     * @return the topic this event was sent from
     */
    String getTopic();

}
