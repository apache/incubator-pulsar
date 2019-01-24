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
#ifndef LIB_UNACKEDMESSAGETRACKERENABLED_H_
#define LIB_UNACKEDMESSAGETRACKERENABLED_H_
#include "lib/UnAckedMessageTrackerInterface.h"
namespace pulsar {

class UnAckedMessageTrackerEnabled : public UnAckedMessageTrackerInterface,
                                     public std::enable_shared_from_this<UnAckedMessageTrackerEnabled> {
   public:
    ~UnAckedMessageTrackerEnabled();
    UnAckedMessageTrackerEnabled(long timeoutMs, const ClientImplPtr, ConsumerImplBase&);
    bool add(const MessageId& m);
    bool remove(const MessageId& m);
    void removeMessagesTill(const MessageId& msgId);
    void removeTopicMessage(const std::string& topic);
    void timeoutHandler();

    void clear();

   private:
    void timeoutHandler(const boost::system::error_code& ec);
    void timeoutHandlerHelper();
    bool isEmpty();
    long size();
    std::set<MessageId> currentSet_;
    std::set<MessageId> oldSet_;
    boost::mutex lock_;
    DeadlineTimerPtr timer_;
    ConsumerImplBase& consumerReference_;
    ClientImplPtr client_;
    long timeoutMs_;
};
}  // namespace pulsar

#endif /* LIB_UNACKEDMESSAGETRACKERENABLED_H_ */
