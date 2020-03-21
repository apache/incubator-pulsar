/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.influxdb.v2;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.util.List;
import java.util.Map;

/**
 * Pulsar sink for InfluxDB2
 */
@Slf4j
public class InfluxDBSink implements Sink<GenericRecord> {

    private WritePrecision writePrecision;

    protected InfluxDBClientBuilder influxDBClientBuilder = new InfluxDBClientBuilderImpl();

    private InfluxDBClient influxDBClient;
    private WriteApi writeApi;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        InfluxDBSinkConfig influxDBSinkConfig = InfluxDBSinkConfig.load(config);
        influxDBSinkConfig.validate();

        influxDBClient = influxDBClientBuilder.build(influxDBSinkConfig);

        val writeOptions = WriteOptions.builder()
                .batchSize(influxDBSinkConfig.getBatchSize())
                .flushInterval(influxDBSinkConfig.getBatchTimeMs())
                .build();
        writeApi = influxDBClient.getWriteApi(writeOptions);
        writePrecision = WritePrecision.fromValue(influxDBSinkConfig.getPrecision().toLowerCase());
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        writeApi.writePoint(buildPoint(record));
    }

    private Point buildPoint(Record<GenericRecord> record) {
        val genericRecord = record.getValue();

        // looking for measurement
        val measurementField = genericRecord.getField("measurement");
        if (null == measurementField) {
            throw new SchemaSerializationException("device is a required field.");
        }
        val measurement = (String) measurementField;

        // looking for timestamp
        long timestamp;
        val timestampField = getFiled(genericRecord, "timestamp");
        if (null == timestampField) {
            timestamp = System.currentTimeMillis();
        } else if (timestampField instanceof Number) {
            timestamp = ((Number) timestampField).longValue();
        } else if (timestampField instanceof String) {
            timestamp = Long.parseLong((String) timestampField);
        } else {
            throw new SchemaSerializationException("Invalid timestamp field");
        }

        val point = Point.measurement(measurement).time(timestamp, writePrecision);

        // Looking for tag fields
        val tagsField = getFiled(genericRecord, "tags");
        if (null != tagsField) {
            if (tagsField instanceof GenericRecord) { // JSONSchema
                GenericRecord tagsRecord = (GenericRecord) tagsField;
                for (Field field : tagsRecord.getFields()) {
                    val fieldName = field.getName();
                    val value = tagsRecord.getField(field);
                    point.addTag(fieldName, (String) value);
                }
            } else if (Map.class.isAssignableFrom(tagsField.getClass())) { // AvroSchema
                Map<Object, Object> tagsMap = (Map<Object, Object>) tagsField;
                tagsMap.forEach((key, value) -> point.addTag(key.toString(), value.toString()));
            } else {
                throw new SchemaSerializationException("Unknown type for 'tags'");
            }
        }

        // Looking for sensor fields
        val columnsField = genericRecord.getField("fields");
        if (columnsField instanceof GenericRecord) { // JSONSchema
            val columnsRecord = (GenericRecord) columnsField;
            for (Field field : columnsRecord.getFields()) {
                val fieldName = field.getName();
                val value = columnsRecord.getField(field);
                addPointField(point, fieldName, value);
            }
        } else if (Map.class.isAssignableFrom(columnsField.getClass())) { // AvroSchema
            val columnsMap = (Map<Object, Object>) columnsField;
            columnsMap.forEach((key, value) -> addPointField(point, key.toString(), value));
        } else {
            throw new SchemaSerializationException("Unknown type for 'fields'");
        }

        return point;
    }

    private void addPointField(Point point, String fieldName, Object value) throws SchemaSerializationException {
        if (value instanceof Number) {
            point.addField(fieldName, (Number) value);
        } else if (value instanceof Boolean) {
            point.addField(fieldName, (Boolean) value);
        } else if (value instanceof String) {
            point.addField(fieldName, (String) value);
        } else if (value instanceof Utf8) {
            point.addField(fieldName, value.toString());
        } else {
            throw new SchemaSerializationException("Unknown value type for field " + fieldName + ". Type: " + value.getClass());
        }
    }

    @Override
    public void close() throws Exception {
        if (null != influxDBClient) {
            writeApi.close();
            influxDBClient.close();
        }
    }

    private Object getFiled(GenericRecord record, String fieldName) {
        List<Field> fields = record.getFields();
        val fieldMatch = fields.stream()
                .filter(field -> fieldName.equals(field.getName()))
                .findAny()
                .orElse(null);
        if (null != fieldMatch) {
            return record.getField(fieldMatch);
        } else {
            return null;
        }
    }
}

