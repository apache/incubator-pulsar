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
package org.apache.pulsar.websocket;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.pulsar.websocket.WebSocketError.FailedToDeserializeFromJSON;
import static org.apache.pulsar.websocket.WebSocketError.PayloadEncodingError;
import static org.apache.pulsar.websocket.WebSocketError.UnknownError;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Enums;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBlockedQuotaExceededError;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBlockedQuotaExceededException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerAck;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.stats.StatsBuckets;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Websocket end-point url handler to handle incoming message coming from client. Websocket end-point url handler to
 * handle incoming message coming from client.
 * <p>
 * On every produced message from client it calls broker to persists it.
 * </p>
 *
 */

public class ProducerHandler extends AbstractWebSocketHandler {

    private Producer<byte[]> producer;
    private final LongAdder numMsgsSent;
    private final LongAdder numMsgsFailed;
    private final LongAdder numBytesSent;
    private final StatsBuckets publishLatencyStatsUSec;
    private volatile long msgPublishedCounter = 0;
    private static final AtomicLongFieldUpdater<ProducerHandler> MSG_PUBLISHED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ProducerHandler.class, "msgPublishedCounter");

    public static final long[] ENTRY_LATENCY_BUCKETS_USEC = { 500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 1000_000 };

    public ProducerHandler(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
        super(service, request, response);
        this.numMsgsSent = new LongAdder();
        this.numBytesSent = new LongAdder();
        this.numMsgsFailed = new LongAdder();
        this.publishLatencyStatsUSec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);

        if (!checkAuth(response)) {
            return;
        }

        try {
            this.producer = getProducerBuilder(service.getPulsarClient()).topic(topic.toString()).create();
            if (!this.service.addProducer(this)) {
                log.warn("[{}:{}] Failed to add producer handler for topic {}", request.getRemoteAddr(),
                        request.getRemotePort(), topic);
            }
        } catch (Exception e) {
            log.warn("[{}:{}] Failed in creating producer on topic {}: {}", request.getRemoteAddr(),
                    request.getRemotePort(), topic, e.getMessage());

            try {
                response.sendError(getErrorCode(e), getErrorMessage(e));
            } catch (IOException e1) {
                log.warn("[{}:{}] Failed to send error: {}", request.getRemoteAddr(), request.getRemotePort(),
                        e1.getMessage(), e1);
            }
        }
    }

    private static int getErrorCode(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return HttpServletResponse.SC_BAD_REQUEST;
        } else if (e instanceof ProducerBusyException) {
            return HttpServletResponse.SC_CONFLICT;
        } else if (e instanceof ProducerBlockedQuotaExceededError || e instanceof ProducerBlockedQuotaExceededException) {
            return HttpServletResponse.SC_SERVICE_UNAVAILABLE;
        } else {
            return HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        }
    }

    private static String getErrorMessage(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return "Invalid query params: " + e.getMessage();
        } else {
            return "Failed to create producer: " + e.getMessage();
        }
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            if (!this.service.removeProducer(this)) {
                log.warn("[{}] Failed to remove producer handler", producer.getTopic());
            }
            producer.closeAsync().thenAccept(x -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Closed producer asynchronously", producer.getTopic());
                }
            }).exceptionally(exception -> {
                log.warn("[{}] Failed to close producer", producer.getTopic(), exception);
                return null;
            });
        }
    }

    @Override
    public void onWebSocketText(String message) {
        ProducerMessage sendRequest;
        byte[] rawPayload = null;
        String requestContext = null;
        try {
            sendRequest = ObjectMapperFactory.getThreadLocal().readValue(message, ProducerMessage.class);
            requestContext = sendRequest.context;
            rawPayload = Base64.getDecoder().decode(sendRequest.payload);
        } catch (IOException e) {
            sendAckResponse(new ProducerAck(FailedToDeserializeFromJSON, e.getMessage(), null, null));
            return;
        } catch (IllegalArgumentException e) {
            String msg = format("Invalid Base64 message-payload error=%s", e.getMessage());
            sendAckResponse(new ProducerAck(PayloadEncodingError, msg, null, requestContext));
            return;
        }

        final long msgSize = rawPayload.length;
        TypedMessageBuilder<byte[]> builder = producer.newMessage();

        try {
            builder.value(rawPayload);
        } catch (SchemaSerializationException e) {
            sendAckResponse(new ProducerAck(PayloadEncodingError, e.getMessage(), null, requestContext));
            return;
        }

        if (sendRequest.properties != null) {
            builder.properties(sendRequest.properties);
        }
        if (sendRequest.key != null) {
            builder.key(sendRequest.key);
        }
        if (sendRequest.replicationClusters != null) {
            builder.replicationClusters(sendRequest.replicationClusters);
        }

        final long now = System.nanoTime();
        builder.sendAsync().thenAccept(msgId -> {
            updateSentMsgStats(msgSize, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - now));
            if (isConnected()) {
                String messageId = Base64.getEncoder().encodeToString(msgId.toByteArray());
                sendAckResponse(new ProducerAck(messageId, sendRequest.context));
            }
        }).exceptionally(exception -> {
            log.warn("[{}] Error occurred while producer handler was sending msg from {}: {}", producer.getTopic(),
                    getRemote().getInetSocketAddress().toString(), exception.getMessage());
            numMsgsFailed.increment();
            sendAckResponse(
                    new ProducerAck(UnknownError, exception.getMessage(), null, sendRequest.context));
            return null;
        });
    }

    public Producer<byte[]> getProducer() {
        return this.producer;
    }

    public long getAndResetNumMsgsSent() {
        return numMsgsSent.sumThenReset();
    }

    public long getAndResetNumBytesSent() {
        return numBytesSent.sumThenReset();
    }

    public long getAndResetNumMsgsFailed() {
        return numMsgsFailed.sumThenReset();
    }

    public long[] getAndResetPublishLatencyStatsUSec() {
        publishLatencyStatsUSec.refresh();
        return publishLatencyStatsUSec.getBuckets();
    }

    public StatsBuckets getPublishLatencyStatsUSec() {
        return this.publishLatencyStatsUSec;
    }

    public long getMsgPublishedCounter() {
        return msgPublishedCounter;
    }

    @Override
    protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception {
        return service.getAuthorizationService().canProduce(topic, authRole, authenticationData);
    }

    private void sendAckResponse(ProducerAck response) {
        try {
            String msg = ObjectMapperFactory.getThreadLocal().writeValueAsString(response);
            getSession().getRemote().sendString(msg);
        } catch (JsonProcessingException e) {
            log.warn("[{}] Failed to generate ack json-response {}", producer.getTopic(), e.getMessage(), e);
        } catch (Exception e) {
            log.warn("[{}] Failed to send ack {}", producer.getTopic(), e.getMessage(), e);
        }
    }

    private void updateSentMsgStats(long msgSize, long latencyUsec) {
        this.publishLatencyStatsUSec.addValue(latencyUsec);
        this.numBytesSent.add(msgSize);
        this.numMsgsSent.increment();
        MSG_PUBLISHED_COUNTER_UPDATER.getAndIncrement(this);
    }

    private ProducerBuilder<byte[]> getProducerBuilder(PulsarClient client) {
        ProducerBuilder<byte[]> builder = client.newProducer()
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition);

        // Set to false to prevent the server thread from being blocked if a lot of messages are pending.
        builder.blockIfQueueFull(false);

        if (queryParams.containsKey("producerName")) {
            builder.producerName(queryParams.get("producerName"));
        }

        if (queryParams.containsKey("initialSequenceId")) {
            builder.initialSequenceId(Long.parseLong("initialSequenceId"));
        }

        if (queryParams.containsKey("hashingScheme")) {
            builder.hashingScheme(HashingScheme.valueOf(queryParams.get("hashingScheme")));
        }

        if (queryParams.containsKey("sendTimeoutMillis")) {
            builder.sendTimeout(Integer.parseInt(queryParams.get("sendTimeoutMillis")), TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("batchingEnabled")) {
            builder.enableBatching(Boolean.parseBoolean(queryParams.get("batchingEnabled")));
        }

        if (queryParams.containsKey("batchingMaxMessages")) {
            builder.batchingMaxMessages(Integer.parseInt(queryParams.get("batchingMaxMessages")));
        }

        if (queryParams.containsKey("maxPendingMessages")) {
            builder.maxPendingMessages(Integer.parseInt(queryParams.get("maxPendingMessages")));
        }

        if (queryParams.containsKey("batchingMaxPublishDelay")) {
            builder.batchingMaxPublishDelay(Integer.parseInt(queryParams.get("batchingMaxPublishDelay")),
                    TimeUnit.MILLISECONDS);
        }

        if (queryParams.containsKey("messageRoutingMode")) {
            checkArgument(
                    Enums.getIfPresent(MessageRoutingMode.class, queryParams.get("messageRoutingMode")).isPresent(),
                    "Invalid messageRoutingMode %s", queryParams.get("messageRoutingMode"));
            MessageRoutingMode routingMode = MessageRoutingMode.valueOf(queryParams.get("messageRoutingMode"));
            if (!MessageRoutingMode.CustomPartition.equals(routingMode)) {
                builder.messageRoutingMode(routingMode);
            }
        }

        if (queryParams.containsKey("compressionType")) {
            checkArgument(Enums.getIfPresent(CompressionType.class, queryParams.get("compressionType")).isPresent(),
                    "Invalid compressionType %s", queryParams.get("compressionType"));
            builder.compressionType(CompressionType.valueOf(queryParams.get("compressionType")));
        }

        return builder;
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerHandler.class);

}
