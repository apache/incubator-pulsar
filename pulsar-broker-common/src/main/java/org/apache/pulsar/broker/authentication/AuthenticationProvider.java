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
package org.apache.pulsar.broker.authentication;

import java.io.Closeable;
import java.io.IOException;

import java.net.SocketAddress;
import java.nio.charset.Charset;
import javax.naming.AuthenticationException;

import javax.net.ssl.SSLSession;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.AuthData;

/**
 * Provider of authentication mechanism
 */
public interface AuthenticationProvider extends Closeable {

    /**
     * Perform initialization for the authentication provider
     *
     * @param config
     *            broker config object
     * @throws IOException
     *             if the initialization fails
     */
    void initialize(ServiceConfiguration config) throws IOException;

    /**
     * @return the authentication method name supported by this provider
     */
    String getAuthMethodName();

    /**
     * Validate the authentication for the given credentials with the specified authentication data
     *
     * @param authData
     *            provider specific authentication data
     * @return the "role" string for the authenticated connection, if the authentication was successful
     * @throws AuthenticationException
     *             if the credentials are not valid
     */
    String authenticate(AuthenticationDataSource authData) throws AuthenticationException;

    /**
     * Create an authentication data provider which provides the data that this broker will be sent to the client.
     */
    default AuthenticationDataSource newAuthDataSource(AuthData authData,
                                                       SocketAddress remoteAddress,
                                                       SSLSession sslSession) throws IOException {
        return new AuthenticationDataCommand(
            new String(authData.getBytes(), Charset.forName("UTF-8")), remoteAddress, sslSession);
    }

    default AuthenticationState newAuthState(AuthenticationDataSource authenticationDataSource) {
        return new OneStageAuthenticationState(authenticationDataSource, this);
    }

}
