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

#ifndef _PULSAR_CONNECTION_POOL_HEADER_
#define  _PULSAR_CONNECTION_POOL_HEADER_

#include <pulsar/Result.h>

#include <lib/ClientConnectionContainer.h>

#include <string>
#include <map>
#include <boost/thread/mutex.hpp>
#include <lib/ClientConnection.h>

namespace pulsar {

class ExecutorService;
typedef boost::shared_ptr<ClientConnectionContainer<ClientConnectionWeakPtr> > ClientConnectionContainerPtr;
class ConnectionPool {
 public:
    ConnectionPool(const ClientConfiguration& conf, ExecutorServiceProviderPtr executorProvider,
                   const AuthenticationPtr& authentication, bool poolConnections = true, size_t connectionsPerBroker = 1);

    Future<Result, ClientConnectionWeakPtr> getConnectionAsync(const std::string& endpoint);

 private:
    ClientConfiguration clientConfiguration_;
    ExecutorServiceProviderPtr executorProvider_;
    AuthenticationPtr authentication_;
    typedef std::map<std::string, ClientConnectionContainerPtr> PoolMap;
    PoolMap pool_;
    bool poolConnections_;
    size_t connectionsPerBroker;
    boost::mutex mutex_;

    friend class ConnectionPoolTest;
};

}

#endif //_PULSAR_CONNECTION_POOL_HEADER_
