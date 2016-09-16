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
package com.yahoo.pulsar.broker.web;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.common.policies.data.ErrorData;

/**
 * Exception used to provide better error messages to clients of the REST API.
 */
@SuppressWarnings("serial")
public class RestException extends WebApplicationException {
    static String getExceptionData(Throwable t) {
        StringWriter writer = new StringWriter();
        writer.append("\n --- An unexpected error occurred in the server ---\n\n");
        writer.append("Message: ").append(t.getMessage()).append("\n\n");
        writer.append("Stacktrace:\n\n");

        t.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    public RestException(Response.Status status, String message) {
        this(status.getStatusCode(), message);
    }

    public RestException(int code, String message) {
        super(Response.status(code).entity(new ErrorData(message)).type(MediaType.APPLICATION_JSON).build());
    }

    public RestException(RestException e) {
        super(Response.status(e.getResponse().getStatus()).entity(e.getResponse().getEntity())
                .type(e.getResponse().getMediaType()).build());
    }

    public RestException(Throwable t) {
        super(Response.status(500).entity(getExceptionData(t)).type(MediaType.TEXT_PLAIN).build());
    }

    public RestException(PulsarAdminException cae) {
        this(cae.getStatusCode(), cae.getHttpError());
    }
}
