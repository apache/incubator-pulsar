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
#include <iostream>
#include <pulsar/Client.h>
#include <utility>

#include <boost/make_shared.hpp>
#include <boost/smart_ptr.hpp>

#include "ClientImpl.h"
#include "Utils.h"
#include "ExecutorService.h"
#include "LogUtils.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

Client::Client(const boost::shared_ptr<ClientImpl> impl) : impl_(impl) {}

Client::Client(const std::string& serviceUrl)
    : impl_(boost::make_shared<ClientImpl>(serviceUrl, ClientConfiguration(), true)) {}

Client::Client(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration)
    : impl_(boost::make_shared<ClientImpl>(serviceUrl, clientConfiguration, true)) {}

Client::Client(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration,
               bool poolConnections)
    : impl_(boost::make_shared<ClientImpl>(serviceUrl, clientConfiguration, poolConnections)) {}

Result Client::createProducer(const std::string& topic, Producer& producer) {
    return createProducer(topic, ProducerConfiguration(), producer);
}

Result Client::createProducer(const std::string& topic, const ProducerConfiguration& conf,
                              Producer& producer) {
    Promise<Result, Producer> promise;
    createProducerAsync(topic, conf, WaitForCallbackValue<Producer>(promise));
    Future<Result, Producer> future = promise.getFuture();

    return future.get(producer);
}

void Client::createProducerAsync(const std::string& topic, CreateProducerCallback callback) {
    createProducerAsync(topic, ProducerConfiguration(), callback);
}

void Client::createProducerAsync(const std::string& topic, ProducerConfiguration conf,
                                 CreateProducerCallback callback) {
    impl_->createProducerAsync(topic, conf, callback);
}

Result Client::subscribe(const std::string& topic, const std::string& consumerName, Consumer& consumer) {
    return subscribe(topic, consumerName, ConsumerConfiguration(), consumer);
}

Result Client::subscribe(const std::string& topic, const std::string& consumerName,
                         const ConsumerConfiguration& conf, Consumer& consumer) {
    Promise<Result, Consumer> promise;
    subscribeAsync(topic, consumerName, conf, WaitForCallbackValue<Consumer>(promise));
    Future<Result, Consumer> future = promise.getFuture();

    return future.get(consumer);
}

void Client::subscribeAsync(const std::string& topic, const std::string& consumerName,
                            SubscribeCallback callback) {
    subscribeAsync(topic, consumerName, ConsumerConfiguration(), callback);
}

void Client::subscribeAsync(const std::string& topic, const std::string& consumerName,
                            const ConsumerConfiguration& conf, SubscribeCallback callback) {
    LOG_DEBUG("Topic is :" << topic);
    impl_->subscribeAsync(topic, consumerName, conf, callback);
}

Result Client::subscribe(const std::vector<std::string>& topics, const std::string& consumerName,
                         Consumer& consumer) {
    return subscribe(topics, consumerName, ConsumerConfiguration(), consumer);
}

Result Client::subscribe(const std::vector<std::string>& topics, const std::string& consumerName,
                         const ConsumerConfiguration& conf, Consumer& consumer) {
    Promise<Result, Consumer> promise;
    subscribeAsync(topics, consumerName, conf, WaitForCallbackValue<Consumer>(promise));
    Future<Result, Consumer> future = promise.getFuture();

    return future.get(consumer);
}

void Client::subscribeAsync(const std::vector<std::string>& topics, const std::string& consumerName,
                            SubscribeCallback callback) {
    subscribeAsync(topics, consumerName, ConsumerConfiguration(), callback);
}

void Client::subscribeAsync(const std::vector<std::string>& topics, const std::string& consumerName,
                            const ConsumerConfiguration& conf, SubscribeCallback callback) {
    impl_->subscribeAsync(topics, consumerName, conf, callback);
}

Result Client::createReader(const std::string& topic, const MessageId& startMessageId,
                            const ReaderConfiguration& conf, Reader& reader) {
    Promise<Result, Reader> promise;
    createReaderAsync(topic, startMessageId, conf, WaitForCallbackValue<Reader>(promise));
    Future<Result, Reader> future = promise.getFuture();

    return future.get(reader);
}

void Client::createReaderAsync(const std::string& topic, const MessageId& startMessageId,
                               const ReaderConfiguration& conf, ReaderCallback callback) {
    impl_->createReaderAsync(topic, startMessageId, conf, callback);
}

Result Client::close() {
    Promise<bool, Result> promise;
    closeAsync(WaitForCallback(promise));

    Result result;
    promise.getFuture().get(result);
    return result;
}

void Client::closeAsync(CloseCallback callback) { impl_->closeAsync(callback); }

void Client::shutdown() { impl_->shutdown(); }
}  // namespace pulsar
