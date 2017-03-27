/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use shared_from_this() file except in compliance with the License.
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

#include "HTTPWrapper.h"

DECLARE_LOG_OBJECT()

namespace pulsar {
    using boost::asio::ip::tcp;
    static const HTTPWrapper::Response EMPTY_RESPONSE = HTTPWrapper::Response();

    std::ostream & operator<<(std::ostream& os, const HTTPWrapper::Request& request) {
        os << HTTPWrapper::getHTTPMethodName(request.method) << " " << request.path << " HTTP/" << request.version << "\r\n";
        os << "Host: " << request.serverUrl.host() << "\r\n";
        std::vector<std::string>::const_iterator iter = request.headers.begin();
        while (iter != request.headers.end()) {
            os << *iter << "\r\n";
            iter++;
        }
        os << request.content << "\r\n";
        return os;
    }

    std::ostream & operator<<(std::ostream &os, const HTTPWrapper::Response& obj) {
        // Don't know why but logger is unable to take the \r in the end and hence giving it a \n to prevent
        // incorrect formatting
        os << "HTTPWrapper::Response [";
        os << "HTTPVersion = " << obj.HTTPVersion << "\n";
        os << ", statusCode = " << obj.statusCode << "\n";

        os << ", statusMessage = " << obj.statusMessage << "\n";
        os << ", headers = {" << "\n";
        std::vector<std::string>::const_iterator iter = obj.headers.begin();
        while (iter != obj.headers.end()) {
            os << *iter << "\n";
            iter++;
        }
        os << "\n}, content = " << obj.content << "\n";
        os << ", retCode = " << obj.retCode << "\n";
        os << ", error_code = " << obj.errCode << "\n";
        os << "}]";
        return os;
    }

    HTTPWrapper::HTTPWrapper(ExecutorServiceProviderPtr executorServiceProviderPtr, HTTPWrapperCallback callback) :
        resolverPtr_(executorServiceProviderPtr->get()->createTcpResolver()),
        requestStreamPtr_(executorServiceProviderPtr->get()->createReadStream()),
        responseStreamPtr_(executorServiceProviderPtr->get()->createReadStream()),
        socketPtr_(executorServiceProviderPtr->get()->createSocket()),
        callback_(callback),
        response_() {
    }

    std::string HTTPWrapper::getHTTPMethodName(const Request::Method& method) {
        switch(method) {
            case Request::GET:
                return "GET";
            case Request::POST:
                return "POST";
            case Request::HEAD:
                return "HEAD";
            case Request::PUT:
                return "PUT";
            case Request::DELETE:
                return "DELETE";
            case Request::OPTIONS:
                return "OPTIONS";
            case Request::CONNECTION:
                return "CONNECTION";
        }
    }


    void HTTPWrapper::createRequest(ExecutorServiceProviderPtr executorServiceProviderPtr,
                                    Request &request,
                                    HTTPWrapperCallback callback) {
        // Since make_shared doesn't work with private/protected constructors
        HTTPWrapperPtr wrapperPtr = HTTPWrapperPtr(new HTTPWrapper(executorServiceProviderPtr, callback));
        wrapperPtr->createRequest(request);
    }

    void HTTPWrapper::createRequest(Request& request) {
        request_ = request;
        std::ostream requestStream(requestStreamPtr_.get());
        requestStream << request;
        LOG_ERROR("HTTP Request Sent: " << request);


        tcp::resolver::query query(request.serverUrl.host(), boost::lexical_cast<std::string>(request.serverUrl.port()));
        LOG_DEBUG("JAI 2");
        resolverPtr_->async_resolve(query,
                                   boost::bind(&HTTPWrapper::handle_resolve, shared_from_this(),
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::iterator));
        LOG_DEBUG("JAI 3");
    }


    void HTTPWrapper::handle_resolve(const boost::system::error_code &err,
                                     tcp::resolver::iterator endpoint_iterator) {
        if (!err) {
            // Attempt a connection to the first endpoint in the list. Each endpoint
            // will be tried until we successfully establish a connection.
            tcp::endpoint endpoint = *endpoint_iterator;
            LOG_DEBUG("JAI 4");
            socketPtr_->async_connect(endpoint,
                                     boost::bind(&HTTPWrapper::handle_connect, shared_from_this(),
                                                 boost::asio::placeholders::error, ++endpoint_iterator));
            LOG_DEBUG("JAI 5");
        } else {
            LOG_ERROR(err.message());
            callback_(shared_from_this());
        }
    }

    void HTTPWrapper::handle_connect(const boost::system::error_code &err, tcp::resolver::iterator endpoint_iterator) {
        if (!err) {
            // The connection was successful. Send the request.
            boost::asio::async_write(*socketPtr_, *requestStreamPtr_,
                                     boost::bind(&HTTPWrapper::handle_write_request, shared_from_this(),
                                                 boost::asio::placeholders::error));
        } else if (endpoint_iterator != tcp::resolver::iterator()) {
            // The connection failed. Try the next endpoint in the list.
            LOG_DEBUG("JAI 7");
            socketPtr_->close();
            tcp::endpoint endpoint = *endpoint_iterator;
            socketPtr_->async_connect(endpoint,
                                     boost::bind(&HTTPWrapper::handle_connect, shared_from_this(),
                                                 boost::asio::placeholders::error, ++endpoint_iterator));
            LOG_DEBUG("JAI 8");
        } else {
            LOG_ERROR(err.message());
            callback_(shared_from_this());
        }
    }

    void HTTPWrapper::handle_write_request(const boost::system::error_code &err) {
        if (!err) {
            // Read the response status line.
            boost::asio::async_read_until(*socketPtr_, *responseStreamPtr_, "\r\n",
                                          boost::bind(&HTTPWrapper::handle_read_status_line, shared_from_this(),
                                                      boost::asio::placeholders::error));
        } else {
            LOG_ERROR(err.message());
            callback_(shared_from_this());
        }
    }

    void HTTPWrapper::handle_read_status_line(const boost::system::error_code &err) {
        // boost::asio::error::eof not handles since HTTP format don't expect EOF before \r\n\r\n
        if (!err) {
            // Check that response is OK.
            std::istream inputStream(responseStreamPtr_.get());
            inputStream >> response_.HTTPVersion;
            inputStream >> response_.statusCode;
            std::getline(inputStream, response_.statusMessage);
            // no headers or non http version
            if (!inputStream || response_.HTTPVersion.substr(0, 5) != "HTTP/") {
                LOG_DEBUG("Invalid response ");
                callback_(shared_from_this());
                return;
            } else if (response_.statusCode != 200) {
                LOG_ERROR("Response returned with status code " << response_.statusCode);
                callback_(shared_from_this());
                return;
            }

            // Read the response headers, which are terminated by a blank line.
            boost::asio::async_read_until(*socketPtr_, *responseStreamPtr_, "\r\n\r\n",
                                          boost::bind(&HTTPWrapper::handle_read_headers, shared_from_this(),
                                                      boost::asio::placeholders::error));
        } else {
            LOG_ERROR(err.message());
            callback_(shared_from_this());
        }
    }

    void HTTPWrapper::handle_read_headers(const boost::system::error_code &err) {
        // boost::asio::error::eof not handles since HTTP format don't expect EOF before \r\n\r\n
        if (!err) {
            std::istream inputStream(responseStreamPtr_.get());
            std::string header;
            // response_.headers guaranteed to have atleast one string since reserve called
            while (std::getline(inputStream, header) && header != "\r") {
                response_.headers.push_back(header);
            }

            if (responseStreamPtr_.get()->size() > 0) {
                // if content doesn't end with \r\n getline doesn't the extract content - remaining characters to be ignored
                // http://www.boost.org/doc/libs/1_55_0/doc/html/boost_asio/reference/async_read_until/overload1.html
            }

            // Start reading remaining data until EOF.
            boost::asio::async_read(*socketPtr_, *responseStreamPtr_,
                                    boost::asio::transfer_at_least(1),
                                    boost::bind(&HTTPWrapper::handle_read_content, shared_from_this(),
                                                boost::asio::placeholders::error));
        } else {
            LOG_ERROR(err.message());
            callback_(shared_from_this());
        }
    }

    void HTTPWrapper::handle_read_content(const boost::system::error_code &err) {
        if (!err) {
            std::istream inputStream(responseStreamPtr_.get());
            inputStream >> response_.content;
            boost::asio::async_read(*socketPtr_, *responseStreamPtr_,
                                    boost::asio::transfer_at_least(1),
                                    boost::bind(&HTTPWrapper::handle_read_content, shared_from_this(),
                                                boost::asio::placeholders::error));
        } else if (err == boost::asio::error::eof) {
            // eof occurs but responseStreamPtr_ not necessarily emptys
            LOG_DEBUG("EOF occured");
            std::istream inputStream(responseStreamPtr_.get());
            inputStream >> response_.content;
            callback_(shared_from_this());
        } else {
            LOG_ERROR(err.message());
            callback_(shared_from_this());
        }
    }
}