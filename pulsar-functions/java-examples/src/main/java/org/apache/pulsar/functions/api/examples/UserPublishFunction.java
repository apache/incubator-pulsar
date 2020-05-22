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
package org.apache.pulsar.functions.api.examples;

import java.util.Optional;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

/**
 * An example demonstrate publishing messages through Context.
 */
public class UserPublishFunction implements Function<String, Void> {

    @Override
    public void prepare(Context context) throws Exception {

    }

    @Override
    public Void process(String input, Context context) {
        Optional<Object> topicToWrite = context.getUserConfigValue("topic");
        if (topicToWrite.isPresent()) {
            try {
                context.newOutputMessage((String) topicToWrite.get(), Schema.STRING).value(input).sendAsync();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}

