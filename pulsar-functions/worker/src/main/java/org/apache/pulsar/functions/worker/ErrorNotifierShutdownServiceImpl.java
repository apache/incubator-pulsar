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

package org.apache.pulsar.functions.worker;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.zookeeper.ZooKeeperSessionWatcher;

@Slf4j
public class ErrorNotifierShutdownServiceImpl implements ErrorNotifier {
    private static final long serialVersionUID = 1L;
    private final ZooKeeperSessionWatcher.ShutdownService shutdownService;

    public ErrorNotifierShutdownServiceImpl(ZooKeeperSessionWatcher.ShutdownService shutdownService) {
        this.shutdownService = shutdownService;
    }

    @Override
    public void triggerError(Throwable th) {
        log.error("Encountered fatal error. Shutting down.", th);
        shutdownService.shutdown(-1);
    }

    @Override
    public void waitForError() throws Exception {
        throw new IllegalArgumentException("Invalid operation for implementation");
    }

    @Override
    public void close() {
        //no-op
    }
}
