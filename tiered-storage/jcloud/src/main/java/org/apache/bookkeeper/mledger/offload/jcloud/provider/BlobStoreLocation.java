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
package org.apache.bookkeeper.mledger.offload.jcloud.provider;

import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Tiered storage blob storage location metadata.
 */
@Data(staticConstructor = "of")
@EqualsAndHashCode
public class BlobStoreLocation implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String providerName;
    private final String region;
    private final String bucket;
    private final String endpoint;

    public BlobStoreLocation(Map<String, String> metadata) {
        this.providerName = getProvider(metadata);
        this.region = getRegion(metadata);
        this.bucket = getBucket(metadata);
        this.endpoint = getEndpoint(metadata);
    }

    String getProvider(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.get(TieredStorageConfiguration.METADATA_FIELD_BLOB_STORE_PROVIDER);
    }

    String getRegion(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.getOrDefault(TieredStorageConfiguration.METADATA_FIELD_REGION, "");
    }

    String getBucket(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.get(TieredStorageConfiguration.METADATA_FIELD_BUCKET);
    }

    String getEndpoint(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.getOrDefault(TieredStorageConfiguration.METADATA_FIELD_ENDPOINT, "");
    }
}
