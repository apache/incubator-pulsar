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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.DataBlockHeaderImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.data.ACL;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.domain.Credentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import com.google.common.util.concurrent.MoreExecutors;

public abstract class BlobStoreManagedLedgerOffloaderBase {

    public final static String BUCKET = "pulsar-unittest";
    protected static final int DEFAULT_BLOCK_SIZE = 5*1024*1024;
    protected static final int DEFAULT_READ_BUFFER_SIZE = 1*1024*1024;
    
    protected final OrderedScheduler scheduler;
    protected final PulsarMockBookKeeper bk;
    protected final JCloudBlobStoreProvider provider;
    protected TieredStorageConfiguration config;
    protected BlobStore blobStore = null;
    
    protected BlobStoreManagedLedgerOffloaderBase() throws Exception {
        scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(5).name("offloader").build();
        bk = new PulsarMockBookKeeper(createMockZooKeeper(), scheduler.chooseThread(this));
        provider = getBlobStoreProvider();
    }

    protected static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<ACL>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(UTF_8), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(UTF_8), dummyAclList,
                CreateMode.PERSISTENT);
        return zk;
    }
    
    /*
     * Determine which BlobStore Provider to test based on the System properties
     */
    protected static JCloudBlobStoreProvider getBlobStoreProvider() {
        if (Boolean.parseBoolean(System.getProperty("testRealAWS", "false"))) {
            return JCloudBlobStoreProvider.AWS_S3;
        } else if (Boolean.parseBoolean(System.getProperty("testRealGCS", "false"))) {
            return JCloudBlobStoreProvider.GOOGLE_CLOUD_STORAGE;
        } else {
            return JCloudBlobStoreProvider.TRANSIENT;
        }
    }
    
    /*
     * Get the credentials to use for the JCloud provider
     * based on the System properties.
     */
    protected static Credentials getBlobStoreCredentials() {
        if (Boolean.parseBoolean(System.getProperty("testRealAWS", "false"))) {
            /* To use this, must config credentials using "aws_access_key_id" as S3ID,
             *  and "aws_secret_access_key" as S3Key. And bucket should exist in default region. e.g.
             *      props.setProperty("S3ID", "AXXXXXXQ");
             *      props.setProperty("S3Key", "HXXXXXß");
             */
            return new Credentials(System.getProperty("S3ID"), System.getProperty("S3Key"));
                    
        } else if (Boolean.parseBoolean(System.getProperty("testRealGCS", "false"))) {
            /*
             * To use this, must config credentials using "client_email" as GCSID and "private_key" as GCSKey.
             * And bucket should exist in default region. e.g.
             *        props.setProperty("GCSID", "5XXXXXXXXXX6-compute@developer.gserviceaccount.com");
             *        props.setProperty("GCSKey", "XXXXXX");
             */
            return new Credentials(System.getProperty("GCSID"), System.getProperty("GCSKey"));
        } else {
            return null;
        }
    }
    
    protected TieredStorageConfiguration getConfiguration(String bucket) {
        Map<String, String> metaData = new HashMap<String, String> ();
        metaData.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, provider.getDriver());
        metaData.put(TieredStorageConfiguration.OFFLOADER_PROPERTY_PREFIX + TieredStorageConfiguration.METADATA_FIELD_REGION, "");
        metaData.put(TieredStorageConfiguration.OFFLOADER_PROPERTY_PREFIX + TieredStorageConfiguration.METADATA_FIELD_BUCKET, bucket);
        metaData.put(TieredStorageConfiguration.OFFLOADER_PROPERTY_PREFIX + TieredStorageConfiguration.METADATA_FIELD_ENDPOINT, "");
        
        TieredStorageConfiguration config = TieredStorageConfiguration.create(metaData);
        config.setProviderCredentials(getBlobStoreCredentials());
        
        return config;
    }

    protected ReadHandle buildReadHandle() throws Exception {
        return buildReadHandle(DEFAULT_BLOCK_SIZE, 1);
    }

    protected ReadHandle buildReadHandle(int maxBlockSize, int blockCount) throws Exception {
        Assert.assertTrue(maxBlockSize > DataBlockHeaderImpl.getDataStartOffset());

        LedgerHandle lh = bk.createLedger(1,1,1, BookKeeper.DigestType.CRC32, "foobar".getBytes());

        int i = 0;
        int bytesWrittenCurrentBlock = DataBlockHeaderImpl.getDataStartOffset();
        int blocksWritten = 1;
        int entries = 0;

        while (blocksWritten < blockCount
               || bytesWrittenCurrentBlock < maxBlockSize/2) {
            byte[] entry = ("foobar"+i).getBytes();
            int sizeInBlock = entry.length + 12 /* ENTRY_HEADER_SIZE */;

            if (bytesWrittenCurrentBlock + sizeInBlock > maxBlockSize) {
                bytesWrittenCurrentBlock = DataBlockHeaderImpl.getDataStartOffset();
                blocksWritten++;
                entries = 0;
            }
            entries++;

            lh.addEntry(entry);
            bytesWrittenCurrentBlock += sizeInBlock;
            i++;
        }

        // workaround mock not closing metadata correctly
        Method close = LedgerMetadata.class.getDeclaredMethod("close", long.class);
        close.setAccessible(true);
        close.invoke(lh.getLedgerMetadata(), lh.getLastAddConfirmed());

        lh.close();

        return bk.newOpenLedgerOp().withLedgerId(lh.getId())
            .withPassword("foobar".getBytes()).withDigestType(DigestType.CRC32).execute().get();
    }

}
