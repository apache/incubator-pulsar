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
package org.apache.pulsar.client.admin.internal;

import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.*;
import org.apache.pulsar.functions.fs.FunctionStatus;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.util.List;

@Slf4j
public class FunctionsImpl extends BaseResource implements Functions {

    private final WebTarget functions;

    public FunctionsImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.functions = web.path("/functions");
    }

    @Override
    public List<String> getFunctions(String tenant, String namespace) throws PulsarAdminException {
        try {
            return request(functions.path(tenant).path(namespace)).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionConfig getFunction(String tenant, String namespace, String function) throws PulsarAdminException {
        try {
            String jsonResponse = request(functions.path(tenant).path(namespace).path(function)).get().readEntity(String.class);
            FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
            JsonFormat.parser().merge(jsonResponse, functionConfigBuilder);
            return functionConfigBuilder.build();
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionStatus getFunctionStatus(String tenant, String namespace, String function) throws PulsarAdminException {
        try {
            return request(functions.path(tenant).path(namespace).path(function).path("status")).get(FunctionStatus.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FileDataBodyPart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM_TYPE));

            mp.bodyPart(new FormDataBodyPart("functionConfig", JsonFormat.printer().print(functionConfig),
                    MediaType.APPLICATION_JSON_TYPE));
            request(functions.path(functionConfig.getTenant()).path(functionConfig.getNamespace()).path(functionConfig.getName()))
                    .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteFunction(String cluster, String namespace, String function) throws PulsarAdminException {
        try {
            request(functions.path(cluster).path(namespace).path(function))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();
            if (fileName != null) {
                mp.bodyPart(new FileDataBodyPart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM_TYPE));
            }
            mp.bodyPart(new FormDataBodyPart("functionConfig", JsonFormat.printer().print(functionConfig),
                        MediaType.APPLICATION_JSON_TYPE));
            request(functions.path(functionConfig.getTenant()).path(functionConfig.getNamespace()).path(functionConfig.getName()))
                    .put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
