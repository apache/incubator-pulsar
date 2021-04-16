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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * This a builder like class for providing a fluent API for graceful shutdown
 *
 * Executors are added with the {@link #shutdown(ExecutorService...)}
 * method. The {@link ExecutorService#shutdown()} method is called immediately.
 *
 * Calling the {@link #handle()} method returns a future which completes when all executors
 * have been terminated. The executors will be polled frequently for completion. If the shutdown times out
 * or the future is cancelled, all executors will be terminated.
 */
public class GracefulExecutorServicesShutdown {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(15);
    private final List<ExecutorService> executorServices = new ArrayList<>();
    private Duration timeout = DEFAULT_TIMEOUT;

    private GracefulExecutorServicesShutdown() {

    }

    /**
     * Initiates a new shutdown for one or many {@link ExecutorService}s.
     *
     * @return a new instance for controlling graceful shutdown
     */
    public static GracefulExecutorServicesShutdown initiate() {
        return new GracefulExecutorServicesShutdown();
    }

    /**
     * Calls {@link ExecutorService#shutdown()} and enlists the executor as part of the
     * shutdown handling.
     *
     * @param executorServices one or many executors to shutdown
     * @return the current instance for controlling graceful shutdown
     */
    public GracefulExecutorServicesShutdown shutdown(ExecutorService... executorServices) {
        for (ExecutorService executorService : executorServices) {
            if (executorService != null) {
                executorService.shutdown();
                this.executorServices.add(executorService);
            }
        }
        return this;
    }

    /**
     * Sets the timeout for graceful shutdown.
     *
     * @param timeout duration for the timeout
     * @return the current instance for controlling graceful shutdown
     */
    public GracefulExecutorServicesShutdown timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Starts the handler for polling frequently for the completed termination of enlisted executors.
     *
     * If the termination times out or the future is cancelled, all active executors will be forcefully
     * terminated by calling {@link ExecutorService#shutdownNow()}.
     *
     * @return a future which completes when all executors have terminated
     */
    public CompletableFuture<Void> handle() {
        return new GracefulExecutorServicesTerminationHandler(timeout.toMillis(), executorServices)
                .startTerminationHandler();
    }
}
