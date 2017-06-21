/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef PULSAR_CONSUMER_STATS_BASE_HEADER
#define PULSAR_CONSUMER_STATS_BASE_HEADER
#include <pulsar/Message.h>
#include <lib/PulsarApi.pb.h>
#include <pulsar/Result.h>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace pulsar {
class ConsumerStatsBase {
 public:
    virtual void receivedMessage(Message&, Result) = 0;
    virtual void messageAcknowledged(Result, proto::CommandAck_AckType) = 0;
    virtual ~ConsumerStatsBase() {}
};

typedef boost::shared_ptr<ConsumerStatsBase> ConsumerStatsBasePtr;
}

#endif // PULSAR_CONSUMER_STATS_BASE_HEADER
