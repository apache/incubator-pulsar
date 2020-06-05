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
package org.apache.pulsar.broker.events;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand;
import org.apache.pulsar.common.nar.NarClassLoader;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * A broker listener with it's classloader.
 */
@Slf4j
@Data
@RequiredArgsConstructor
public class SafeBrokerEventListenerWithClassLoader implements BrokerEventListener {

    private final BrokerEventListener listener;
    private final NarClassLoader classLoader;

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
        try {
            this.listener.onPulsarCommand(command, cnx);
        } catch (Throwable e) {
            log.error("Fail to execute pulsar command on broker listener", e);
        }
    }

    @Override
    public void onWebServiceRequest(ServletRequest request, ServletResponse response, FilterChain chain) {
        try {
            this.listener.onWebServiceRequest(request, response, chain);
        } catch (Throwable e) {
            log.error("Fail to execute webservice request on broker listener", e);
        }
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        this.listener.initialize(conf);
    }

    @Override
    public void close() {
        listener.close();
        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the broker listener class loader", e);
        }
    }
}
