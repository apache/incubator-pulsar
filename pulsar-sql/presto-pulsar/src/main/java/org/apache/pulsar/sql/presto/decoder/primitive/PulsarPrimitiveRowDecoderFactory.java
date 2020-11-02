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
package org.apache.pulsar.sql.presto.decoder.primitive;

import io.airlift.log.Logger;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.*;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.sql.presto.PulsarColumnHandle;
import org.apache.pulsar.sql.presto.PulsarColumnMetadata;
import org.apache.pulsar.sql.presto.PulsarRowDecoderFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class PulsarPrimitiveRowDecoderFactory implements PulsarRowDecoderFactory {

    private static final Logger log = Logger.get(PulsarPrimitiveRowDecoderFactory.class);

    @Override
    public RowDecoder createRowDecoder(SchemaInfo schemaInfo, Set<DecoderColumnHandle> columns) {
        if (columns.size() == 1) {
            return new PulsarPrimitiveRowDecoder(columns.iterator().next());
        } else {
            throw new RuntimeException("Primitive type must has only one  ColumnHandle ");
        }
    }

    @Override
    public List<ColumnMetadata> extractColumnMetadata(SchemaInfo schemaInfo, PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        ColumnMetadata valueColumn = new PulsarColumnMetadata(
                PulsarColumnMetadata.getColumnName(handleKeyValueType, "__value__"),
                parsePrimitivePrestoType("__value__", schemaInfo.getType()),
                "The value of the message with primitive type schema", null, false, false,
                handleKeyValueType, new PulsarColumnMetadata.DecoderExtraInfo("__value__", null, null));
        return Arrays.asList(valueColumn);
    }


    private Type parsePrimitivePrestoType(String fieldName, SchemaType pulsarType) {
        switch (pulsarType) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT8:
                return TinyintType.TINYINT;
            case INT16:
                return SmallintType.SMALLINT;
            case INT32:
                return IntegerType.INTEGER;
            case INT64:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case NONE:
            case BYTES:
                return VarbinaryType.VARBINARY;
            case STRING:
                return VarcharType.VARCHAR;
            case DATE:
                return DateType.DATE;
            case TIME:
                return TimeType.TIME;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            default:
                log.error("Cannot convert type: %s", pulsarType);
                return null;
        }

    }
}
