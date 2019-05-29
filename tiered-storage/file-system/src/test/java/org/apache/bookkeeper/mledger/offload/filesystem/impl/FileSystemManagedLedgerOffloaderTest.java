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
package org.apache.bookkeeper.mledger.offload.filesystem.impl;


import com.google.common.util.concurrent.MoreExecutors;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.filesystem.FileStoreTestBase;
import org.apache.bookkeeper.mledger.offload.filesystem.FileSystemEntryBytesReader;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FileSystemManagedLedgerOffloaderTest extends FileStoreTestBase {
    private final PulsarMockBookKeeper bk;
    private long objectLength = FileSystemEntryBytesReader.getDataHeaderLength();
    private String topic = "public/default/persistent/testOffload";
    private String storagePath = createStoragePath(topic);
    private LedgerHandle lh;
    private ReadHandle toWrite;
    private long idOfExceedReadBuffer;
    private final int numberOfEntries = 600;
    private  Map<String, String> map = new HashMap<>();

    public FileSystemManagedLedgerOffloaderTest() throws Exception {
        this.bk = new PulsarMockBookKeeper(createMockZooKeeper(), scheduler.chooseThread(this));
        this.toWrite = buildReadHandle();
        map.put("ManagedLedgerName", topic);
    }

    private static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<ACL>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(UTF_8), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(UTF_8), dummyAclList,
                CreateMode.PERSISTENT);
        return zk;
    }

    private ReadHandle buildReadHandle() throws Exception {

        lh = bk.createLedger(1,1,1, BookKeeper.DigestType.CRC32, "foobar".getBytes());

        int i = 0;
        int blocksWritten = 1;
        while (blocksWritten <= numberOfEntries) {
            byte[] entry = ("foobar"+i).getBytes();
            objectLength += entry.length + FileSystemEntryBytesReader.getEntryHeaderSize();
            if (objectLength > getReadBufferSize() + FileSystemEntryBytesReader.getDataHeaderLength()) {
                idOfExceedReadBuffer = lh.readLastEntry().getEntryId();
            }

            blocksWritten++;
            lh.addEntry(entry);
            i++;
        }
        lh.close();

        return bk.newOpenLedgerOp().withLedgerId(lh.getId())
                .withPassword("foobar".getBytes()).withDigestType(DigestType.CRC32).execute().get();
    }
    @Test
    public void testOffload() throws Exception {
        LedgerOffloader offloader = fileSystemManagedLedgerOffloader;
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, map).get();
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(getURI()), configuration, getUserName());
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(createDataFilePath(storagePath, lh.getId(), uuid)));
        Assert.assertEquals(objectLength, fileStatus.getLen());
    }
    @Test
    public void testOffloadAndRead() throws Exception {
        LedgerOffloader offloader = fileSystemManagedLedgerOffloader;
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, map).get();
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, map).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());
        LedgerEntries toTestEntries = toTest.read(0, idOfExceedReadBuffer - 1);
        LedgerEntries toWriteEntries = toWrite.read(0,idOfExceedReadBuffer - 1);
        Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();
        Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
        while(toTestIter.hasNext()) {
            LedgerEntry toWriteEntry = toWriteIter.next();
            LedgerEntry toTestEntry = toTestIter.next();

            Assert.assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
            Assert.assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
            Assert.assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
            Assert.assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
        }
        toTestEntries = toTest.read(0, idOfExceedReadBuffer);
        toWriteEntries = toWrite.read(0,idOfExceedReadBuffer);
        toTestIter = toTestEntries.iterator();
        toWriteIter = toWriteEntries.iterator();
        while(toTestIter.hasNext()) {
            LedgerEntry toWriteEntry = toWriteIter.next();
            LedgerEntry toTestEntry = toTestIter.next();

            Assert.assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
            Assert.assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
            Assert.assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
            Assert.assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
        }
        toTestEntries = toTest.read(0, numberOfEntries - 1);
        toWriteEntries = toWrite.read(0, numberOfEntries - 1);
        toTestIter = toTestEntries.iterator();
        toWriteIter = toWriteEntries.iterator();
        while(toTestIter.hasNext()) {
            LedgerEntry toWriteEntry = toWriteIter.next();
            LedgerEntry toTestEntry = toTestIter.next();

            Assert.assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
            Assert.assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
            Assert.assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
            Assert.assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
        }
    }
    @Test
    public void testRatherThanWrite() throws Exception {
        LedgerOffloader offloader = fileSystemManagedLedgerOffloader;
        UUID uuid = UUID.randomUUID();
        offloader.offload(toWrite, uuid, map).get();
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(getURI()), configuration, getUserName());
        Assert.assertEquals(true, fileSystem.exists(new Path(createDataFilePath(storagePath, lh.getId(), uuid))));
        Assert.assertEquals(true, fileSystem.exists(new Path(createIndexFilePath(storagePath, lh.getId(), uuid))));
        offloader.deleteOffloaded(lh.getId(), uuid, map);
        Assert.assertEquals(false, fileSystem.exists(new Path(createDataFilePath(storagePath, lh.getId(), uuid))));
        Assert.assertEquals(false, fileSystem.exists(new Path(createIndexFilePath(storagePath, lh.getId(), uuid))));
    }

    @Test
    public void testDeleteOffload() throws Exception {
        LedgerOffloader offloader = fileSystemManagedLedgerOffloader;
        UUID uuid = UUID.randomUUID();
        Map<String, String> map = new HashMap<>();
        map.put("ManagedLedgerName", topic);
        offloader.offload(toWrite, uuid, map).get();
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, map).get();

        offloader.deleteOffloaded(lh.getId(), uuid, map);
    }

    private String createStoragePath(String managedLedgerName) {
        return basePath + "/" + managedLedgerName + "/";
    }

    private String createIndexFilePath(String storagePath, long ledgerId, UUID uuid) {
        return storagePath + ledgerId + "-" + uuid + ".index";
    }

    private String createDataFilePath(String storagePath, long ledgerId, UUID uuid) {
        return storagePath + ledgerId + "-" + uuid + ".log";
    }
}
