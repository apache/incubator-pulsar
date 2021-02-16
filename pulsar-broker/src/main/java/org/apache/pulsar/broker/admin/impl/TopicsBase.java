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
package org.apache.pulsar.broker.admin.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.rest.RestMessagePublishContext;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.websocket.data.ProducerAck;
import org.apache.pulsar.websocket.data.ProducerAcks;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;

/**
 * Contains methods used by REST api to producer/consumer/read messages to/from pulsar topics.
 */
@Slf4j
public class TopicsBase extends PersistentTopicsBase {

    private static String defaultProducerName = "RestProducer";

    protected  void publishMessages(AsyncResponse asyncResponse, ProducerMessages request,
                                           boolean authoritative) {
        String topic = topicName.getPartitionedTopicName();
        if (pulsar().getOwningTopics().containsKey(topic) || !findOwnerBrokerForTopic(authoritative, asyncResponse)) {
            // if we've done look up or or after look up this broker owns some of the partitions
            // then proceed to publish message else asyncResponse will be complete by look up.
            addOrGetSchemaForTopic(getSchemaData(request.getKeySchema(), request.getValueSchema()),
                    request.getSchemaVersion() == -1? null : new LongSchemaVersion(request.getSchemaVersion()))
            .thenAccept(schemaMeta -> {
                // Both schema version and schema data are necessary.
                if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                    publishMessagesToMultiplePartitions(topicName, request, pulsar().getOwningTopics().get(topic),
                            asyncResponse, AutoConsumeSchema.getSchema(schemaMeta.getLeft().toSchemaInfo()),
                            schemaMeta.getRight());
                } else {
                    asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                }
            }).exceptionally(e -> {
                if (log.isDebugEnabled()) {
                    log.warn("Fail to add or retrieve schema: " + e.getLocalizedMessage());
                }
                asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                return null;
            });
        }
    }

    protected void publishMessagesToPartition(AsyncResponse asyncResponse, ProducerMessages request,
                                                     boolean authoritative, int partition) {
        if (topicName.isPartitioned()) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Topic name can't contain "
                    + "'-partition-' suffix."));
        }
        String topic = topicName.getPartitionedTopicName();
        // If broker owns the partition then proceed to publish message, else do look up.
        if ((pulsar().getOwningTopics().containsKey(topic) && pulsar().getOwningTopics().get(topic).contains(partition))
                || !findOwnerBrokerForTopic(authoritative, asyncResponse)) {
            addOrGetSchemaForTopic(getSchemaData(request.getKeySchema(), request.getValueSchema()),
                    request.getSchemaVersion() == -1? null : new LongSchemaVersion(request.getSchemaVersion()))
            .thenAccept(schemaMeta -> {
                // Both schema version and schema data are necessary.
                if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                    SchemaInfo schemaInfo = schemaMeta.getLeft().toSchemaInfo();
                    Schema schema;
                    if (schemaInfo.getType() == SchemaType.KEY_VALUE) {

                    } else {

                    }
                    publishMessagesToSinglePartition(topicName, request, partition, asyncResponse,
                            AutoConsumeSchema.getSchema(schemaMeta.getLeft().toSchemaInfo()), schemaMeta.getRight());
                } else {
                    asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                }
            }).exceptionally(e -> {
                if (log.isDebugEnabled()) {
                    log.warn("Fail to add or retrieve schema: " + e.getLocalizedMessage());
                }
                asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                return null;
            });
        }
    }

    private CompletableFuture<PositionImpl> publishSingleMessageToPartition(String topic, Message message) {
        CompletableFuture<PositionImpl> publishResult = new CompletableFuture<>();
        pulsar().getBrokerService().getTopic(topic, false)
        .thenAccept(t -> {
            // TODO: Check message backlog
            if (!t.isPresent()) {
                // Topic not found, and remove from owning partition list.
                publishResult.completeExceptionally(new BrokerServiceException.TopicNotFoundException("Topic not "
                        + "owned by current broker."));
                TopicName topicName = TopicName.get(topic);
                pulsar().getOwningTopics().get(topicName.getPartitionedTopicName())
                        .remove(topicName.getPartitionIndex());
            } else {
                t.get().publishMessage(messageToByteBuf(message),
                RestMessagePublishContext.get(publishResult, t.get(), System.nanoTime()));
            }
        });

        return publishResult;
    }

    private void publishMessagesToSinglePartition(TopicName topicName, ProducerMessages request,
                                                  int partition, AsyncResponse asyncResponse,
                                                  Schema schema, SchemaVersion schemaVersion) {
        try {
            String producerName = (null == request.getProducerName() || request.getProducerName().isEmpty())
                    ? defaultProducerName : request.getProducerName();
            List<Message> messages = buildMessage(request, schema, producerName);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProducerAck> produceMessageResults = new ArrayList<>();
            for (int index = 0; index < messages.size(); index++) {
                ProducerAck produceMessageResult = new ProducerAck();
                produceMessageResult.setMessageId(partition + "");
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToPartition(topicName.getPartition(partition).toString(),
                        messages.get(index)));
            }
            FutureUtil.waitForAll(publishResults).thenRun(() -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
            }).exceptionally(e -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
                return null;
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail publish message with rest produce message request for topic  {}: {} ",
                        topicName, e.getCause());
            }
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, e.getMessage()));
        }
    }

    private void publishMessagesToMultiplePartitions(TopicName topicName, ProducerMessages request,
                                                     ConcurrentOpenHashSet<Integer> partitionIndexes,
                                                     AsyncResponse asyncResponse, Schema schema,
                                                     SchemaVersion schemaVersion) {
        try {
            String producerName = (null == request.getProducerName() || request.getProducerName().isEmpty())
                    ? defaultProducerName : request.getProducerName();
            List<Message> messages = buildMessage(request, schema, producerName);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProducerAck> produceMessageResults = new ArrayList<>();
            List<Integer> owningPartitions = partitionIndexes.values();
            for (int index = 0; index < messages.size(); index++) {
                ProducerAck produceMessageResult = new ProducerAck();
                produceMessageResult.setMessageId(owningPartitions.get(index % (int) partitionIndexes.size()) + "");
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToPartition(topicName.getPartition(owningPartitions
                                .get(index % (int) partitionIndexes.size())).toString(),
                    messages.get(index)));
            }
            FutureUtil.waitForAll(publishResults).thenRun(() -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
            }).exceptionally(e -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
                return null;
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail publish message with rest produce message request for topic  {}: {} ",
                        topicName, e.getCause());
            }
            e.printStackTrace();
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, e.getMessage()));
        }
    }

    private void processPublishMessageResults(List<ProducerAck> produceMessageResults,
                                              List<CompletableFuture<PositionImpl>> publishResults) {
        // process publish message result
        for (int index = 0; index < publishResults.size(); index++) {
            try {
                PositionImpl position = publishResults.get(index).get();
                MessageId messageId = new MessageIdImpl(position.getLedgerId(), position.getEntryId(),
                        Integer.parseInt(produceMessageResults.get(index).getMessageId()));
                produceMessageResults.get(index).setMessageId(messageId.toString());
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.warn("Fail publish [{}] message with rest produce message request for topic  {}: {} ",
                            index, topicName);
                }
                if (e instanceof BrokerServiceException.TopicNotFoundException) {
                    // Topic ownership might changed, force to look up again.
                    pulsar().getOwningTopics().remove(topicName.getPartitionedTopicName());
                }
                extractException(e, produceMessageResults.get(index));
            }
        }
    }

    private void extractException(Exception e, ProducerAck produceMessageResult) {
        if (!(e instanceof BrokerServiceException.TopicFencedException && e instanceof ManagedLedgerException)) {
            produceMessageResult.setErrorCode(2);
        } else {
            produceMessageResult.setErrorCode(1);
        }
        produceMessageResult.setErrorMsg(e.getMessage());
    }

    // Look up topic owner for given topic.
    // Return if asyncResponse has been completed.
    private boolean findOwnerBrokerForTopic(boolean authoritative, AsyncResponse asyncResponse) {
        PartitionedTopicMetadata metadata = internalGetPartitionedMetadata(authoritative, false);
        List<String> redirectAddresses = Collections.synchronizedList(new ArrayList<>());
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        List<CompletableFuture<Void>> lookupFutures = new ArrayList<>();
        if (!topicName.isPartitioned() && metadata.partitions > 1) {
            // Partitioned topic with multiple partitions, need to do look up for each partition.
            for (int index = 0; index < metadata.partitions; index++) {
                lookupFutures.add(lookUpBrokerForTopic(topicName.getPartition(index),
                        authoritative, redirectAddresses));
            }
        } else {
            // Non-partitioned topic or specific topic partition.
            lookupFutures.add(lookUpBrokerForTopic(topicName, authoritative, redirectAddresses));
        }

        FutureUtil.waitForAll(lookupFutures)
        .thenRun(() -> {
            // Current broker doesn't own the topic or any partition of the topic, redirect client to a broker
            // that own partition of the topic or know who own partition of the topic.
            if (!pulsar().getOwningTopics().containsKey(topicName.getPartitionedTopicName())) {
                if (redirectAddresses.isEmpty()) {
                    // No broker to redirect, means look up for some partitions failed,
                    // client should retry with other brokers.
                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Can't find owner of given topic."));
                    future.complete(true);
                } else {
                    // Redirect client to other broker owns the topic or know which broker own the topic.
                    try {
                        URI redirectURI = new URI(String.format("%s%s", redirectAddresses.get(0), uri.getPath(false)));
                        asyncResponse.resume(Response.temporaryRedirect(redirectURI).build());
                        future.complete(true);
                    } catch (URISyntaxException | NullPointerException e) {
                        log.error("Error in preparing redirect url with rest produce message request for topic  {}: {}",
                                topicName, e.getMessage(), e);
                        asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR,
                                "Fail to redirect client request."));
                        future.complete(true);
                    }
                }
            } else {
                future.complete(false);
            }
        }).exceptionally(e -> {
            if (log.isDebugEnabled()) {
                log.warn("Fail to look up topic: " + e.getCause());
            }
            asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Error look up topic: "
                    + e.getLocalizedMessage()));
            future.complete(true);
            return null;
        });
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail to lookup topic for rest produce message request for topic {}.", topicName.toString());
            }
            return true;
        }
    }

    // Look up topic owner for non-partitioned topic or single topic partition.
    private CompletableFuture<Void> lookUpBrokerForTopic(TopicName partitionedTopicName,
                                                         boolean authoritative, List<String> redirectAddresses) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<Optional<LookupResult>> lookupFuture = pulsar().getNamespaceService()
                .getBrokerServiceUrlAsync(partitionedTopicName,
                        LookupOptions.builder().authoritative(authoritative).loadTopicsInBundle(false).build());

        lookupFuture.thenAccept(optionalResult -> {
            if (optionalResult == null || !optionalResult.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.warn("Fail to lookup topic for rest produce message request for topic {}.",
                            partitionedTopicName);
                }
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses, future);
                return;
            }

            LookupResult result = optionalResult.get();
            if (result.getLookupData().getHttpUrl().equals(pulsar().getWebServiceAddress())) {
                pulsar().getBrokerService().getLookupRequestSemaphore().release();
                // Current broker owns the topic, add to owning topic.
                if (log.isDebugEnabled()) {
                    log.warn("Complete topic look up for rest produce message request for topic {}, "
                                    + "current broker is owner broker: {}",
                            partitionedTopicName, result.getLookupData());
                }
                pulsar().getOwningTopics().computeIfAbsent(partitionedTopicName.getPartitionedTopicName(),
                        (key) -> new ConcurrentOpenHashSet<Integer>()).add(partitionedTopicName.getPartitionIndex());
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses, future);
            } else {
                // Current broker doesn't own the topic or doesn't know who own the topic.
                if (log.isDebugEnabled()) {
                    log.warn("Complete topic look up for rest produce message request for topic {}, "
                                    + "current broker is not owner broker: {}",
                            partitionedTopicName, result.getLookupData());
                }
                if (result.isRedirect()) {
                    // Redirect lookup.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), false), redirectAddresses, future);
                } else {
                    // Found owner for topic.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), true), redirectAddresses, future);
                }
            }
        }).exceptionally(exception -> {
            log.warn("Failed to lookup broker with rest produce message request for topic {}: {}",
                    partitionedTopicName, exception.getMessage());
            completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses, future);
            return null;
        });
        return future;
    }

    private CompletableFuture<Pair<SchemaData, SchemaVersion>> addOrGetSchemaForTopic(SchemaData schemaData,
                                                                                      SchemaVersion schemaVersion) {
        CompletableFuture<Pair<SchemaData, SchemaVersion>> future = new CompletableFuture<>();
        if (null != schemaVersion) {
            String id = TopicName.get(topicName.getPartitionedTopicName()).getSchemaName();
            SchemaRegistry.SchemaAndMetadata schemaAndMetadata;
            try {
                schemaAndMetadata = pulsar().getSchemaRegistryService().getSchema(id, schemaVersion).get();
                future.complete(Pair.of(schemaAndMetadata.schema, schemaAndMetadata.version));
            } catch (InterruptedException | ExecutionException e) {
                future.complete(Pair.of(null, null));
            }
        } else if (null != schemaData) {
            SchemaVersion sv;
            try {
                sv = addSchema(schemaData).get();
                future.complete(Pair.of(schemaData, sv));
            } catch (InterruptedException | ExecutionException e) {
                future.complete(Pair.of(null, null));
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        } else {
            future.complete(Pair.of(null, null));
        }
        return future;
    }

    private CompletableFuture<SchemaVersion> addSchema(SchemaData schemaData) {
        // Only need to add to first partition the broker owns since the schema id in schema registry are
        // same for all partitions which is the partitionedTopicName
        List<Integer> partitions = pulsar().getOwningTopics().get(topicName.getPartitionedTopicName()).values();
        CompletableFuture<SchemaVersion> result = new CompletableFuture<>();
        for (int index = 0; index < partitions.size(); index++) {
            CompletableFuture<SchemaVersion> future = new CompletableFuture<>();
            String topicPartitionName = topicName.getPartition(partitions.get(index)).toString();
            pulsar().getBrokerService().getTopic(topicPartitionName, false)
            .thenAccept(topic -> {
                if (!topic.isPresent()) {
                    future.completeExceptionally(new BrokerServiceException.TopicNotFoundException(
                            "Topic " + topicPartitionName + " not found"));
                } else {
                    topic.get().addSchema(schemaData).thenAccept(schemaVersion -> future.complete(schemaVersion))
                    .exceptionally(exception -> {
                        future.completeExceptionally(exception);
                        return null;
                    });
                }
            });
            try {
                result.complete(future.get());
                break;
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Fail to add schema to topic " + topicName.getPartitionedTopicName()
                            + " for partition " + partitions.get(index) + " for REST produce request.");
                }
            }
        }
        // Not able to add schema to any partition
        if (!result.isDone()) {
            result.completeExceptionally(new SchemaException("Unable to add schema " + schemaData
                    + " to topic " + topicName.getPartitionedTopicName()));
        }
        return result;
    }

    private SchemaData getSchemaData(String keySchema, String valueSchema) {
        try {
            SchemaInfo valueSchemaInfo = (valueSchema == null || valueSchema.isEmpty())
                    ? StringSchema.utf8().getSchemaInfo() : ObjectMapperFactory.getThreadLocal()
                    .readValue(Base64.getDecoder().decode(valueSchema), SchemaInfo.class);
            if (null == valueSchemaInfo.getName()) {
                valueSchemaInfo.setName(valueSchemaInfo.getType().toString());
            }
            // Value schema only
            if (keySchema == null || keySchema.isEmpty()) {
                return SchemaData.builder()
                        .data(valueSchemaInfo.getSchema())
                        .isDeleted(false)
                        .user("Rest Producer")
                        .timestamp(System.currentTimeMillis())
                        .type(valueSchemaInfo.getType())
                        .props(valueSchemaInfo.getProperties())
                        .build();
            } else {
                // Key_Value schema
                SchemaInfo keySchemaInfo = ObjectMapperFactory.getThreadLocal()
                        .readValue(Base64.getDecoder().decode(keySchema), SchemaInfo.class);
                if (null == keySchemaInfo.getName()) {
                    keySchemaInfo.setName(keySchemaInfo.getType().toString());
                }
                SchemaInfo schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo("KVSchema-"
                                + topicName.getPartitionedTopicName(),
                        keySchemaInfo, valueSchemaInfo,
                        KeyValueEncodingType.SEPARATED);
                return SchemaData.builder()
                        .data(schemaInfo.getSchema())
                        .isDeleted(false)
                        .user("Rest Producer")
                        .timestamp(System.currentTimeMillis())
                        .type(schemaInfo.getType())
                        .props(schemaInfo.getProperties())
                        .build();
            }
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail to parse schema info for rest produce request with key schema {} and value schema {}"
                        , keySchema, valueSchema);
            }
            return null;
        }
    }

    // Convert message to ByteBuf
    public ByteBuf messageToByteBuf(Message message) {
        checkArgument(message instanceof MessageImpl, "Message must be type of MessageImpl.");

        MessageImpl msg = (MessageImpl) message;
        MessageMetadata messageMetadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();
        messageMetadata.setCompression(CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        messageMetadata.setUncompressedSize(payload.readableBytes());

        ByteBuf byteBuf = null;
        try {
            byteBuf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, payload);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return byteBuf;
    }

    // Build pulsar message from REST request.
    private List<Message> buildMessage(ProducerMessages producerMessages, Schema schema,
                                       String producerName) {
        List<ProducerMessage> messages;
        List<Message> pulsarMessages = new ArrayList<>();

        messages = producerMessages.getMessages();
        for (ProducerMessage message : messages) {
            try {
                MessageMetadata messageMetadata = new MessageMetadata();
                messageMetadata.setProducerName(producerName);
                messageMetadata.setPublishTime(System.currentTimeMillis());
                messageMetadata.setSequenceId(message.getSequenceId());
                if (null != message.getReplicationClusters()) {
                    messageMetadata.addAllReplicateTos(message.getReplicationClusters());
                }

                if (null != message.getProperties()) {
                    messageMetadata.addAllProperties(message.getProperties().entrySet().stream().map(entry -> {
                        org.apache.pulsar.common.api.proto.KeyValue keyValue =
                                new org.apache.pulsar.common.api.proto.KeyValue();
                        keyValue.setKey(entry.getKey());
                        keyValue.setValue(entry.getValue());
                        return keyValue;
                    }).collect(Collectors.toList()));
                }
                if (null != message.getKey()) {
                    // If has key schema, encode partition key, else use plain text.
                    if (schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
                        KeyValueSchema kvSchema = (KeyValueSchema) schema;
                        messageMetadata.setPartitionKey(
                                Base64.getEncoder().encodeToString(encodeWithSchema(message.getKey(),
                                        kvSchema.getKeySchema())));
                        messageMetadata.setPartitionKeyB64Encoded(true);
                    } else {
                        messageMetadata.setPartitionKey(message.getKey());
                        messageMetadata.setPartitionKeyB64Encoded(false);
                    }
                }
                if (null != message.getEventTime() && !message.getEventTime().isEmpty()) {
                    messageMetadata.setEventTime(Long.valueOf(message.getEventTime()));
                }
                if (message.isDisableReplication()) {
                    messageMetadata.clearReplicateTo();
                    messageMetadata.addReplicateTo("__local__");
                }
                if (message.getDeliverAt() != 0 && messageMetadata.hasEventTime()) {
                    messageMetadata.setDeliverAtTime(message.getDeliverAt());
                } else if (message.getDeliverAfterMs() != 0) {
                    messageMetadata.setDeliverAtTime(messageMetadata.getEventTime() + message.getDeliverAfterMs());
                }
                if (schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
                    KeyValueSchema kvSchema = (KeyValueSchema) schema;
                    pulsarMessages.add(MessageImpl.create(messageMetadata,
                            ByteBuffer.wrap(encodeWithSchema(message.getPayload(), kvSchema.getValueSchema())),
                            schema));
                } else {
                    pulsarMessages.add(MessageImpl.create(messageMetadata,
                            ByteBuffer.wrap(encodeWithSchema(message.getPayload(), schema)), schema));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return pulsarMessages;
    }

    private byte[] processJSONMsg(String msg, GenericJsonSchema schema) throws JsonProcessingException {
        GenericJsonRecord genericJsonRecord = new
                GenericJsonRecord(null, null,
                ObjectMapperFactory.getThreadLocal().readTree(msg), schema.getSchemaInfo());

        return schema.encode(genericJsonRecord);
    }

    private byte[] encodeWithSchema(String input, Schema schema) {
        try {
            switch (schema.getSchemaInfo().getType()) {
                case INT8:
                    return schema.encode(new Byte(input));
                case INT16:
                    return schema.encode(Short.parseShort(input));
                case INT32:
                    return schema.encode(Integer.parseInt(input));
                case INT64:
                    return schema.encode(Long.parseLong(input));
                case STRING:
                    return schema.encode(input);
                case FLOAT:
                    return schema.encode(Float.parseFloat(input));
                case DOUBLE:
                    return schema.encode(Double.parseDouble(input));
                case BOOLEAN:
                    return schema.encode(Boolean.parseBoolean(input));
                case BYTES:
                    return schema.encode(input.getBytes());
                case DATE:
                    return schema.encode(DateFormat.getDateInstance().parse(input));
                case TIME:
                    return schema.encode(new Time(Long.parseLong(input)));
                case TIMESTAMP:
                    return schema.encode(new Timestamp(Long.parseLong(input)));
                case INSTANT:
                    return schema.encode(Instant.parse(input));
                case LOCAL_DATE:
                    return schema.encode(LocalDate.parse(input));
                case LOCAL_TIME:
                    return schema.encode(LocalTime.parse(input));
                case LOCAL_DATE_TIME:
                    return schema.encode(LocalDateTime.parse(input));
                case JSON:
                    return schema.encode(new
                            GenericJsonRecord(null, null,
                            ObjectMapperFactory.getThreadLocal().readTree(input), schema.getSchemaInfo()));
                case AVRO:
                case PROTOBUF_NATIVE:
                case KEY_VALUE:
                default:
                    throw new PulsarClientException.InvalidMessageException("");
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("");
            }
            return null;
        }
    }

    private synchronized void completeLookup(Pair<List<String>, Boolean> result, List<String> redirectAddresses,
                                              CompletableFuture<Void> future) {
        pulsar().getBrokerService().getLookupRequestSemaphore().release();
        if (!result.getLeft().isEmpty()) {
            if (result.getRight()) {
                // If address is for owner of topic partition, add to head and it'll have higher priority
                // compare to broker for look redirect.
                redirectAddresses.add(0, isRequestHttps() ? result.getLeft().get(1) : result.getLeft().get(0));
            } else {
                redirectAddresses.add(redirectAddresses.size(), isRequestHttps()
                        ? result.getLeft().get(1) : result.getLeft().get(0));
            }
        }
        future.complete(null);
    }
}
