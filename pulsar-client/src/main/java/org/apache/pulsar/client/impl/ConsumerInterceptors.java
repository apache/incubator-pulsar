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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A container that hold the list {@link org.apache.pulsar.client.api.ConsumerInterceptor} and wraps calls to the chain
 * of custom interceptors.
 */
public class ConsumerInterceptors<T> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerInterceptors.class);

    private final List<ConsumerInterceptor<T>> interceptors;

    public ConsumerInterceptors(List<ConsumerInterceptor<T>> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * This is called just before the message is returned by {@link Consumer#receive()},
     * {@link MessageListener#received(Consumer, Message)} or the {@link java.util.concurrent.CompletableFuture}
     * returned by {@link Consumer#receiveAsync()} completes.
     * <p>
     * This method calls {@link ConsumerInterceptor#beforeConsume(Message)} for each interceptor. Messages returned
     * from each interceptor get passed to beforeConsume() of the next interceptor in the chain of interceptors.
     * <p>
     * This method does not throw exceptions. If any of the interceptors in the chain throws an exception, it gets
     * caught and logged, and next interceptor in int the chain is called with 'messages' returned by the previous
     * successful interceptor beforeConsume call.
     *
     * @param message message to be consume by the client.
     * @return messages that are either modified by interceptors or same as messages passed to this method.
     */
    public Message<T> beforeConsume(Message<T> message) {
        Message<T> interceptorMessage = message;
        for (ConsumerInterceptor<T> interceptor : this.interceptors) {
            try {
                interceptorMessage = interceptor.beforeConsume(interceptorMessage);
            } catch (Exception e) {
                log.warn("Error executing interceptor beforeConsume callback ", e);
            }
        }
        return interceptorMessage;
    }

    /**
     * This is called when acknowledge request return from the broker.
     * <p>
     * This method calls {@link ConsumerInterceptor#onAcknowledge(Message, Throwable)} method for each interceptor.
     * <p>
     * This method does not throw exceptions. Exceptions thrown by any of interceptors in the chain are logged, but not propagated.
     *
     * @param message message to acknowledge.
     * @param cause exception returned by broker.
     */
    public void onAcknowledge(Message<T> message, Throwable cause) {
        for (ConsumerInterceptor<T> interceptor : interceptors) {
            try {
                interceptor.onAcknowledge(message, cause);
            } catch (Exception e) {
                log.warn("Error executing interceptor onAcknowledge callback ", e);
            }
        }
    }

    /**
     * This is called when acknowledge cumulative request return from the broker.
     * <p>
     * This method calls {@link ConsumerInterceptor#onAcknowledgeCumulative(List, Throwable)} (Message, Throwable)} method for each interceptor.
     * <p>
     * This method does not throw exceptions. Exceptions thrown by any of interceptors in the chain are logged, but not propagated.
     *
     * @param messages messages to acknowledge.
     * @param cause exception returned by broker.
     */
    public void onAcknowledgeCumulative(List<Message<T>> messages, Throwable cause) {
        for (ConsumerInterceptor<T> interceptor : interceptors) {
            try {
                interceptor.onAcknowledgeCumulative(messages, cause);
            } catch (Exception e) {
                log.warn("Error executing interceptor onAcknowledgeCumulative callback ", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        this.interceptors.forEach(interceptor -> {
            try {
                interceptor.close();
            } catch (Exception e) {
                log.error("Fail to close consumer interceptor ", e);
            }
        });
    }

}
