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
#include "utils.h"

struct Consumer_MessageListener {
    PyObject* _pyListener;

    Consumer_MessageListener(py::object pyListener) :
        _pyListener(pyListener.ptr()) {
        Py_XINCREF(_pyListener);
    }

    Consumer_MessageListener(const Consumer_MessageListener& other) {
        _pyListener = other._pyListener;
        Py_XINCREF(_pyListener);
    }

    Consumer_MessageListener& operator=(const Consumer_MessageListener& other) {
        _pyListener = other._pyListener;
        Py_XINCREF(_pyListener);
        return *this;
    }

    virtual ~Consumer_MessageListener() {
        Py_XDECREF(_pyListener);
    }

    void operator()(Consumer consumer, const Message& msg) {
        PyGILState_STATE state = PyGILState_Ensure();

        try {
            py::call<void>(_pyListener, py::object(&consumer), py::object(&msg));
        } catch (py::error_already_set e) {
            PyErr_Print();
        }

        PyGILState_Release(state);
    }
};

static ConsumerConfiguration& ConsumerConfiguration_setMessageListener(ConsumerConfiguration& conf,
                                                                py::object pyListener) {
    conf.setMessageListener(Consumer_MessageListener(pyListener));
    return conf;
}

static ClientConfiguration& ClientConfiguration_setAuthentication(ClientConfiguration& conf,
                                                                  py::object authentication) {
    AuthenticationWrapper wrapper = py::extract<AuthenticationWrapper>(authentication);
    conf.setAuth(wrapper.auth);
    return conf;
}

void export_config() {
    using namespace boost::python;

    class_<ClientConfiguration>("ClientConfiguration")
            .def("authentication", &ClientConfiguration_setAuthentication, return_self<>())
            .def("operation_timeout_seconds", &ClientConfiguration::getOperationTimeoutSeconds)
            .def("operation_timeout_seconds", &ClientConfiguration::setOperationTimeoutSeconds, return_self<>())
            .def("io_threads", &ClientConfiguration::getIOThreads)
            .def("io_threads", &ClientConfiguration::setIOThreads, return_self<>())
            .def("message_listener_threads", &ClientConfiguration::getMessageListenerThreads)
            .def("message_listener_threads", &ClientConfiguration::setMessageListenerThreads, return_self<>())
            .def("concurrent_lookup_requests", &ClientConfiguration::getConcurrentLookupRequest)
            .def("concurrent_lookup_requests", &ClientConfiguration::setConcurrentLookupRequest, return_self<>())
            .def("log_conf_file_path", &ClientConfiguration::getLogConfFilePath, return_value_policy<copy_const_reference>())
            .def("log_conf_file_path", &ClientConfiguration::setLogConfFilePath, return_self<>())
            .def("use_tls", &ClientConfiguration::isUseTls)
            .def("use_tls", &ClientConfiguration::setUseTls, return_self<>())
            .def("tls_trust_certs_file_path", &ClientConfiguration::getTlsTrustCertsFilePath)
            .def("tls_trust_certs_file_path", &ClientConfiguration::setTlsTrustCertsFilePath, return_self<>())
            .def("tls_allow_insecure_connection", &ClientConfiguration::isTlsAllowInsecureConnection)
            .def("tls_allow_insecure_connection", &ClientConfiguration::setTlsAllowInsecureConnection, return_self<>())
            ;

    class_<ProducerConfiguration>("ProducerConfiguration")
            .def("send_timeout_millis", &ProducerConfiguration::getSendTimeout)
            .def("send_timeout_millis", &ProducerConfiguration::setSendTimeout, return_self<>())
            .def("compression_type", &ProducerConfiguration::getCompressionType)
            .def("compression_type", &ProducerConfiguration::setCompressionType, return_self<>())
            .def("max_pending_messages", &ProducerConfiguration::getMaxPendingMessages)
            .def("max_pending_messages", &ProducerConfiguration::setMaxPendingMessages, return_self<>())
            .def("block_if_queue_full", &ProducerConfiguration::getBlockIfQueueFull)
            .def("block_if_queue_full", &ProducerConfiguration::setBlockIfQueueFull, return_self<>())
            .def("partitions_routing_mode", &ProducerConfiguration::getPartitionsRoutingMode)
            .def("partitions_routing_mode", &ProducerConfiguration::setPartitionsRoutingMode, return_self<>())
            .def("batching_enabled", &ProducerConfiguration::getBatchingEnabled, return_value_policy<copy_const_reference>())
            .def("batching_enabled", &ProducerConfiguration::setBatchingEnabled, return_self<>())
            .def("batching_max_messages", &ProducerConfiguration::getBatchingMaxMessages, return_value_policy<copy_const_reference>())
            .def("batching_max_messages", &ProducerConfiguration::setBatchingMaxMessages, return_self<>())
            .def("batching_max_allowed_size_in_bytes", &ProducerConfiguration::getBatchingMaxAllowedSizeInBytes, return_value_policy<copy_const_reference>())
            .def("batching_max_allowed_size_in_bytes", &ProducerConfiguration::setBatchingMaxAllowedSizeInBytes, return_self<>())
            .def("batching_max_publish_delay_ms", &ProducerConfiguration::getBatchingMaxPublishDelayMs, return_value_policy<copy_const_reference>())
            .def("batching_max_publish_delay_ms", &ProducerConfiguration::setBatchingMaxPublishDelayMs, return_self<>())
            ;

    class_<ConsumerConfiguration>("ConsumerConfiguration")
            .def("consumer_type", &ConsumerConfiguration::getConsumerType)
            .def("consumer_type", &ConsumerConfiguration::setConsumerType, return_self<>())
            .def("message_listener", &ConsumerConfiguration_setMessageListener, return_self<>())
            .def("receiver_queue_size", &ConsumerConfiguration::getReceiverQueueSize)
            .def("receiver_queue_size", &ConsumerConfiguration::setReceiverQueueSize)
            .def("consumer_name", &ConsumerConfiguration::getConsumerName, return_value_policy<copy_const_reference>())
            .def("consumer_name", &ConsumerConfiguration::setConsumerName)
            .def("unacked_messages_timeout_ms", &ConsumerConfiguration::getUnAckedMessagesTimeoutMs)
            .def("unacked_messages_timeout_ms", &ConsumerConfiguration::setUnAckedMessagesTimeoutMs)
            .def("broker_consumer_stats_cache_time_ms", &ConsumerConfiguration::getBrokerConsumerStatsCacheTimeInMs)
            .def("broker_consumer_stats_cache_time_ms", &ConsumerConfiguration::setBrokerConsumerStatsCacheTimeInMs)
            ;
}
