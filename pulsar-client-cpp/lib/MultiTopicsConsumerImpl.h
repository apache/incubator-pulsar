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
#ifndef PULSAR_MULTI_TOPICS_CONSUMER_HEADER
#define PULSAR_MULTI_TOPICS_CONSUMER_HEADER
#include "ConsumerImpl.h"
#include "ClientImpl.h"
#include "BlockingQueue.h"
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include "boost/enable_shared_from_this.hpp"
#include "ConsumerImplBase.h"
#include "lib/UnAckedMessageTrackerDisabled.h"
#include <lib/Latch.h>
#include <lib/MultiTopicsBrokerConsumerStatsImpl.h>
#include <lib/TopicName.h>
#include <lib/NamespaceName.h>

namespace pulsar {
typedef boost::shared_ptr<Promise<Result, Consumer>> ConsumerSubResultPromisePtr;
typedef boost::function<void(Result result)> ResultCallback;

class MultiTopicsConsumerImpl;
class MultiTopicsConsumerImpl : public ConsumerImplBase,
                                public boost::enable_shared_from_this<MultiTopicsConsumerImpl> {
   public:
    enum MultiTopicsConsumerState
    {
        Pending,
        Ready,
        Closing,
        Closed,
        Failed
    };
    MultiTopicsConsumerImpl(ClientImplPtr client, const std::vector<std::string>& topics,
                            const std::string& subscriptionName, TopicNamePtr topicName,
                            const ConsumerConfiguration& conf, const LookupServicePtr lookupServicePtr_);
    virtual ~MultiTopicsConsumerImpl();
    virtual Future<Result, ConsumerImplBaseWeakPtr> getConsumerCreatedFuture();
    virtual const std::string& getSubscriptionName() const;
    virtual const std::string& getTopic() const;
    virtual const std::string& getName() const;
    virtual Result receive(Message& msg);
    virtual Result receive(Message& msg, int timeout);
    virtual void unsubscribeAsync(ResultCallback callback);
    virtual void acknowledgeAsync(const MessageId& msgId, ResultCallback callback);
    virtual void acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback);
    virtual void closeAsync(ResultCallback callback);
    virtual void start();
    virtual void shutdown();
    virtual bool isClosed();
    virtual bool isOpen();
    virtual Result pauseMessageListener();
    virtual Result resumeMessageListener();
    virtual void redeliverUnacknowledgedMessages();
    virtual int getNumOfPrefetchedMessages() const;
    virtual void getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback);
    void handleGetConsumerStats(Result, BrokerConsumerStats, LatchPtr, MultiTopicsBrokerConsumerStatsPtr,
                                size_t, BrokerConsumerStatsCallback);
    // return first topic name when all topics name valid, or return null pointer
    static boost::shared_ptr<TopicName> topicNamesValid(const std::vector<std::string>& topics);
    void unsubscribeOneTopicAsync(const std::string& topic, ResultCallback callback);
    Future<Result, Consumer> subscribeOneTopicAsync(const std::string& topic);
    // not supported
    virtual void seekAsync(const MessageId& msgId, ResultCallback callback);

   private:
    const ClientImplPtr client_;
    const std::string subscriptionName_;
    std::string consumerStr_;
    std::string topic_;
    NamespaceNamePtr namespaceName_;
    const ConsumerConfiguration conf_;
    typedef std::map<std::string, ConsumerImplPtr> ConsumerMap;
    ConsumerMap consumers_;
    std::map<std::string, int> topicsPartitions_;
    boost::mutex mutex_;
    MultiTopicsConsumerState state_;
    boost::shared_ptr<std::atomic<int>> allTopicPartitionsNumber_;
    LookupServicePtr lookupServicePtr_;
    BlockingQueue<Message> messages_;
    ExecutorServicePtr listenerExecutor_;
    MessageListener messageListener_;
    Promise<Result, ConsumerImplBaseWeakPtr> multiTopicsConsumerCreatedPromise_;
    UnAckedMessageTrackerScopedPtr unAckedMessageTrackerPtr_;
    const std::vector<std::string>& topics_;

    /* methods */
    void setState(MultiTopicsConsumerState state);
    void handleSinglePartitionConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                              unsigned int partitionIndex);
    void handleSingleConsumerClose(Result result, std::string& topicPartitionName, CloseCallback callback);
    void notifyResult(CloseCallback closeCallback);
    void messageReceived(Consumer consumer, const Message& msg);
    void internalListener(Consumer consumer);
    void receiveMessages();

    void handleOneTopicSubscribe(Result result, Consumer consumer, const std::string& topic,
                                 boost::shared_ptr<std::atomic<int>> topicsCreated, const int topicsNumber);
    void subscribeTopicPartitions(const Result result, const LookupDataResultPtr partitionMetadata,
                                  TopicNamePtr topicName, const std::string& consumerName,
                                  ConsumerConfiguration conf,
                                  ConsumerSubResultPromisePtr topicSubResultPromise);
    void handleSingleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                     boost::shared_ptr<std::atomic<int>> partitionsCreated,
                                     const int partitionsNumber,
                                     ConsumerSubResultPromisePtr topicSubResultPromise);
    void handleUnsubscribeAsync(Result result, boost::shared_ptr<std::atomic<int>> consumerUnsubed,
                                ResultCallback callback);
    void handleUnsubscribeOneTopicAsync(Result result, boost::shared_ptr<std::atomic<int>> consumerUnsubed,
                                        int numberPartitions, TopicNamePtr topicNamePtr,
                                        std::string& topicPartitionName, ResultCallback callback);
};

}  // namespace pulsar
#endif  // PULSAR_MULTI_TOPICS_CONSUMER_HEADER
