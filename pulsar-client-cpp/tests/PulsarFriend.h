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

#include <pulsar/BatchMessageId.h>
#include <lib/ProducerImpl.h>
#include <string>

using std::string;

namespace pulsar{
class PulsarFriend {
    public:
    static int getBatchIndex(const BatchMessageId& mId) {
        return mId.batchIndex_;
    }

    static PublisherStatsBasePtr getPublisherStatsPtr(Producer producer) {
        ProducerImpl* producerImpl = dynamic_cast<ProducerImpl*>(producer.impl_.get());
        return producerImpl->publisherStatsBasePtr_;     
    }
};
}
