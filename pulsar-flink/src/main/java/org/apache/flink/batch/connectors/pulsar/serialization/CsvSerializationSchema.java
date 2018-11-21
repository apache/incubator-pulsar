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
package org.apache.flink.batch.connectors.pulsar.serialization;

import org.apache.commons.csv.CSVFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;

public class CsvSerializationSchema<T extends Tuple> implements SerializationSchema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvSerializationSchema.class);
    private static final long serialVersionUID = -3379119592495232636L;

    private static final int STRING_WRITER_INITIAL_BUFFER_SIZE = 256;

    public CsvSerializationSchema() {
    }

    @Override
    public byte[] serialize(T t) {
        StringWriter stringWriter = null;
        try {
            Object[] fieldsValues = new Object[t.getArity()];
            for(int index = 0; index < t.getArity();  index++) {
                fieldsValues[index] = (t.getField(index));
            }

            stringWriter = new StringWriter(STRING_WRITER_INITIAL_BUFFER_SIZE);
            CSVFormat.DEFAULT.withRecordSeparator("").printRecord(stringWriter, fieldsValues);
        } catch (IOException e) {
            LOG.error("Error while serializing the record to Csv : ", e);
        }

        return stringWriter.toString().getBytes();
    }

}
