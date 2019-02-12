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
package org.apache.pulsar.proxy.server;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.SslContextRefresher;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;

/**
 * Initialize service channel handlers.
 *
 */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";
    private final ProxyService proxyService;
    // private final SslContext serverSslCtx;
    private final SslContextRefresher serverSslCtxRefresher;
    private final SslContext clientSslCtx;
    private final boolean enableTls;

    public ServiceChannelInitializer(ProxyService proxyService, ProxyConfiguration serviceConfig, boolean enableTls)
            throws Exception {
        super();
        this.proxyService = proxyService;
        this.enableTls = enableTls;

        if (enableTls) {
            serverSslCtxRefresher = new SslContextRefresher(serviceConfig.isTlsAllowInsecureConnection(),
                    serviceConfig.getTlsTrustCertsFilePath(), serviceConfig.getTlsCertificateFilePath(),
                    serviceConfig.getTlsKeyFilePath(), serviceConfig.getTlsCiphers(), serviceConfig.getTlsProtocols(),
                    serviceConfig.isTlsRequireTrustedClientCertOnConnect(), proxyService.getWorkerGroup(),
                    serviceConfig.getCertRefreshCheckDurationInMins(), TimeUnit.MINUTES);
        } else {
            this.serverSslCtxRefresher = null;
        }

        if (serviceConfig.isTlsEnabledWithBroker()) {
            AuthenticationDataProvider authData = null;

            if (!isEmpty(serviceConfig.getBrokerClientAuthenticationPlugin())) {
                authData = AuthenticationFactory.create(serviceConfig.getBrokerClientAuthenticationPlugin(),
                        serviceConfig.getBrokerClientAuthenticationParameters()).getAuthData();
            }

            if (authData != null && authData.hasDataForTls()) {
                this.clientSslCtx = SecurityUtility.createNettySslContextForClient(
                        serviceConfig.isTlsAllowInsecureConnection(), serviceConfig.getBrokerClientTrustCertsFilePath(),
                        (X509Certificate[]) authData.getTlsCertificates(), authData.getTlsPrivateKey());
            } else {
                this.clientSslCtx = SecurityUtility.createNettySslContextForClient(
                        serviceConfig.isTlsAllowInsecureConnection(),
                        serviceConfig.getBrokerClientTrustCertsFilePath());
            }
        } else {
            this.clientSslCtx = null;
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, serverSslCtxRefresher.get().newHandler(ch.alloc()));
        }

        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(PulsarDecoder.MaxFrameSize, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", new ProxyConnection(proxyService, clientSslCtx));
    }
}
