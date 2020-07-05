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
package org.apache.pulsar.functions.worker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.Map;

public class KeyValueNestedGenericsFunction implements Function<KeyValue<String, KeyValue<String, Map<String, Integer>>>
        , KeyValue<String, KeyValue<String, Map<String, Integer>>>> {
    @Override
    public KeyValue<String, KeyValue<String, Map<String, Integer>>> process(KeyValue<String, KeyValue<String, Map<String, Integer>>> input, Context context) throws Exception {
        return input;
    }

    public static class KeyValueStudentFunction implements Function<KeyValue<KeyValue<String, Integer>, Student>, KeyValue<KeyValue<String, Integer>, Student>>{
        @Override
        public KeyValue<KeyValue<String, Integer>, Student> process(KeyValue<KeyValue<String, Integer>, Student> input, Context context) throws Exception {
            return input;
        }
    }

    public static class KeyValueStudentWithAvroFunction implements Function<KeyValue<KeyValue<Student, Student>, Student>, KeyValue<KeyValue<Student, Student>, Student>>{
        @Override
        public KeyValue<KeyValue<Student, Student>, Student> process(KeyValue<KeyValue<Student, Student>, Student> input, Context context) throws Exception {
            return input;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Student{
        private String name;
        private String address;
    }
}
