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
package org.apache.pulsar.sql.presto;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Multi version generic schema provider by guava cache.
 */
public class PulsarSqlSchemaInfoProvider implements SchemaInfoProvider {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSqlSchemaInfoProvider.class);

    private final TopicName topicName;

    private final PulsarAdmin pulsarAdmin;

    private final LoadingCache<byte[], SchemaInfo> cache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<byte[], SchemaInfo>() {
                @Override
                public SchemaInfo load(byte[] schemaVersion) throws Exception {
                    return loadSchema(schemaVersion);
                }
            });

    public PulsarSqlSchemaInfoProvider(TopicName topicName, PulsarAdmin pulsarAdmin) {
        this.topicName = topicName;
        this.pulsarAdmin = pulsarAdmin;
    }

    @Override
    public SchemaInfo getSchemaByVersion(byte[] schemaVersion) {
        try {
            if (null == schemaVersion) {
                return null;
            }
            return cache.get(schemaVersion);
        } catch (ExecutionException e) {
            LOG.error("Can't get generic schema for topic {} schema version {}",
                    topicName.toString(), new String(schemaVersion, StandardCharsets.UTF_8), e);
            throw new RuntimeException("Can't get generic schema for topic " + topicName.toString());
        }
    }

    @Override
    public SchemaInfo getLatestSchema() {
        try {
            return pulsarAdmin.schemas()
                    .getSchemaInfo(topicName.toString());
        } catch (PulsarAdminException e) {
            LOG.error("Can't get current schema for topic {}",
                    topicName.toString(), e);
            throw new RuntimeException("Can't get current schema for topic " + topicName.toString());
        }
    }

    @Override
    public String getTopicName() {
        return topicName.getLocalName();
    }

    private SchemaInfo loadSchema(byte[] schemaVersion) throws PulsarAdminException {
        return pulsarAdmin.schemas()
                .getSchemaInfo(topicName.toString(), bytes2Long(schemaVersion));
    }

    public static long bytes2Long(byte[] byteNum) {
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }
}
