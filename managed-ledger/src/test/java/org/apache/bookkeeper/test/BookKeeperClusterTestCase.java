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
/**
 * This file is derived from BookKeeperClusterTestCase from Apache BookKeeper
 * http://bookkeeper.apache.org
 */

package org.apache.bookkeeper.test;

import io.netty.buffer.ByteBufAllocator;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.metastore.InMemoryMetaStore;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * A class runs several bookie servers for testing.
 */
public abstract class BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(BookKeeperClusterTestCase.class);

    // ZooKeeper related variables
    protected ZooKeeperUtil zkUtil = new ZooKeeperUtil();
    protected ZooKeeper zkc;

    // BookKeeper related variables
    protected List<File> tmpDirs = new LinkedList<File>();
    protected List<BookieServer> bs = new LinkedList<BookieServer>();
    protected List<ServerConfiguration> bsConfs = new LinkedList<ServerConfiguration>();
    protected int numBookies;
    protected BookKeeperTestClient bkc;

    protected ServerConfiguration baseConf = new ServerConfiguration();
    protected ClientConfiguration baseClientConf = new ClientConfiguration();

    private final Map<BookieServer, AutoRecoveryMain> autoRecoveryProcesses = new HashMap<BookieServer, AutoRecoveryMain>();

    private boolean isAutoRecoveryEnabled;

    protected ExecutorService executor;

    public BookKeeperClusterTestCase(int numBookies) {
        this.numBookies = numBookies;
    }

    @BeforeMethod
    public void setUp() throws Exception {
        // enable zookeeper `zookeeper.4lw.commands.whitelist`
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
        executor = Executors.newCachedThreadPool();
        InMemoryMetaStore.reset();
        setMetastoreImplClass(baseConf);
        setMetastoreImplClass(baseClientConf);

        try {
            String zkPath = changeLedgerPath();
            // start zookeeper service
            startZKCluster(zkPath);
            // start bookkeeper service
            startBKCluster(zkPath);

            zkc.create(zkPath + "/managed-ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }
    }

    protected String changeLedgerPath() {
        return "";
    }

    @AfterMethod
    public void tearDown() throws Exception {
        // stop bookkeeper service
        stopBKCluster();
        // stop zookeeper service
        stopZKCluster();
        executor.shutdown();
    }

    /**
     * Start zookeeper cluster
     *
     * @throws Exception
     */
    protected void startZKCluster() throws Exception {
        startZKCluster("");
    }

    protected void startZKCluster(String path) throws Exception {
        zkUtil.startServer(path);
        zkc = zkUtil.getZooKeeperClient();
    }

    /**
     * Stop zookeeper cluster
     *
     * @throws Exception
     */
    protected void stopZKCluster() throws Exception {
        zkUtil.killServer();
    }

    /**
     * Start cluster. Also, starts the auto recovery process for each bookie, if isAutoRecoveryEnabled is true.
     *
     * @throws Exception
     */
    protected void startBKCluster(String ledgerPath) throws Exception {
        baseClientConf.setMetadataServiceUri("zk://" + zkUtil.getZooKeeperConnectString() + ledgerPath + "/ledgers");
        baseClientConf.setUseV2WireProtocol(true);
        baseClientConf.setEnableDigestTypeAutodetection(true);
        baseClientConf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        if (numBookies > 0) {
            bkc = new BookKeeperTestClient(baseClientConf);
        }

        // Create Bookie Servers (B1, B2, B3)
        for (int i = 0; i < numBookies; i++) {
            if (ledgerPath != "") {
                ServerConfiguration configuration = newServerConfiguration(ledgerPath + "/ledgers");
                startBookie(configuration, ledgerPath + "/ledgers");
            }else {
                startNewBookie();
            }
        }
    }

    /**
     * Stop cluster. Also, stops all the auto recovery processes for the bookie cluster, if isAutoRecoveryEnabled is
     * true.
     *
     * @throws Exception
     */
    protected void stopBKCluster() throws Exception {
        if (bkc != null) {
            bkc.close();
        }

        for (BookieServer server : bs) {
            server.shutdown();
            AutoRecoveryMain autoRecovery = autoRecoveryProcesses.get(server);
            if (autoRecovery != null && isAutoRecoveryEnabled()) {
                autoRecovery.shutdown();
                LOG.debug("Shutdown auto recovery for bookieserver:" + server.getLocalAddress());
            }
        }
        bs.clear();
        for (File f : tmpDirs) {
            FileUtils.deleteDirectory(f);
        }
    }

    protected ServerConfiguration newServerConfiguration() throws Exception {
        return newServerConfiguration("");
    }

    protected ServerConfiguration newServerConfiguration(String ledgerRootPath) throws Exception {
        File f = File.createTempFile("bookie", "test");
        tmpDirs.add(f);
        f.delete();
        f.mkdir();
        
        int port = 0;
        return newServerConfiguration(port, zkUtil.getZooKeeperConnectString(), f, new File[] { f }, ledgerRootPath);
    }

    protected ClientConfiguration newClientConfiguration() {
        return new ClientConfiguration(baseConf);
    }

    protected ServerConfiguration newServerConfiguration(int port, String zkServers, File journalDir,
            File[] ledgerDirs, String ledgerRootPath) {
        ServerConfiguration conf = new ServerConfiguration(baseConf);
        conf.setBookiePort(port);
        if (ledgerRootPath != "") {
            conf.setMetadataServiceUri("zk://" + zkUtil.getZooKeeperConnectString() + ledgerRootPath);
        }else {
            conf.setZkServers(zkServers);
        }
        conf.setJournalDirName(journalDir.getPath());
        conf.setAllowLoopback(true);
        conf.setFlushInterval(60 * 1000);
        conf.setGcWaitTime(60 * 1000);
        conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        String[] ledgerDirNames = new String[ledgerDirs.length];
        for (int i = 0; i < ledgerDirs.length; i++) {
            ledgerDirNames[i] = ledgerDirs[i].getPath();
        }
        conf.setLedgerDirNames(ledgerDirNames);
        conf.setLedgerStorageClass("org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
        return conf;
    }

    /**
     * Get bookie address for bookie at index
     */
    public BookieSocketAddress getBookie(int index) throws Exception {
        if (bs.size() <= index || index < 0) {
            throw new IllegalArgumentException(
                    "Invalid index, there are only " + bs.size() + " bookies. Asked for " + index);
        }
        return bs.get(index).getLocalAddress();
    }

    /**
     * Kill a bookie by its socket address. Also, stops the autorecovery process for the corresponding bookie server, if
     * isAutoRecoveryEnabled is true.
     *
     * @param addr
     *            Socket Address
     * @return the configuration of killed bookie
     * @throws InterruptedException
     */
    public ServerConfiguration killBookie(InetSocketAddress addr) throws Exception {
        BookieServer toRemove = null;
        int toRemoveIndex = 0;
        for (BookieServer server : bs) {
            if (server.getLocalAddress().equals(addr)) {
                server.shutdown();
                toRemove = server;
                break;
            }
            ++toRemoveIndex;
        }
        if (toRemove != null) {
            stopAutoRecoveryService(toRemove);
            bs.remove(toRemove);
            return bsConfs.remove(toRemoveIndex);
        }
        return null;
    }

    /**
     * Kill a bookie by index. Also, stops the respective auto recovery process for this bookie, if
     * isAutoRecoveryEnabled is true.
     *
     * @param index
     *            Bookie Index
     * @return the configuration of killed bookie
     * @throws InterruptedException
     * @throws IOException
     */
    public ServerConfiguration killBookie(int index) throws Exception {
        if (index >= bs.size()) {
            throw new IOException("Bookie does not exist");
        }
        BookieServer server = bs.get(index);
        server.shutdown();
        stopAutoRecoveryService(server);
        bs.remove(server);
        return bsConfs.remove(index);
    }

    /**
     * Sleep a bookie
     *
     * @param addr
     *            Socket Address
     * @param seconds
     *            Sleep seconds
     * @return Count Down latch which will be counted down when sleep finishes
     * @throws InterruptedException
     * @throws IOException
     */
    public CountDownLatch sleepBookie(InetSocketAddress addr, final int seconds) throws Exception {
        for (final BookieServer bookie : bs) {
            if (bookie.getLocalAddress().equals(addr)) {
                final CountDownLatch l = new CountDownLatch(1);
                Thread sleeper = new Thread() {
                    @Override
                    public void run() {
                        try {
                            bookie.suspendProcessing();
                            l.countDown();
                            Thread.sleep(seconds * 1000);
                            bookie.resumeProcessing();
                        } catch (Exception e) {
                            LOG.error("Error suspending bookie", e);
                        }
                    }
                };
                sleeper.start();
                return l;
            }
        }
        throw new IOException("Bookie not found");
    }

    /**
     * Sleep a bookie until I count down the latch
     *
     * @param addr
     *            Socket Address
     * @param l
     *            Latch to wait on
     * @throws InterruptedException
     * @throws IOException
     */
    public void sleepBookie(InetSocketAddress addr, final CountDownLatch l) throws Exception {
        for (final BookieServer bookie : bs) {
            if (bookie.getLocalAddress().equals(addr)) {
                Thread sleeper = new Thread() {
                    @Override
                    public void run() {
                        try {
                            bookie.suspendProcessing();
                            l.await();
                            bookie.resumeProcessing();
                        } catch (Exception e) {
                            LOG.error("Error suspending bookie", e);
                        }
                    }
                };
                sleeper.start();
                return;
            }
        }
        throw new IOException("Bookie not found");
    }

    /**
     * Restart bookie servers. Also restarts all the respective auto recovery process, if isAutoRecoveryEnabled is true.
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     * @throws BookieException
     */
    public void restartBookies() throws Exception {
        restartBookies(null);
    }

    /**
     * Restart bookie servers using new configuration settings. Also restart the respective auto recovery process, if
     * isAutoRecoveryEnabled is true.
     *
     * @param newConf
     *            New Configuration Settings
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     * @throws BookieException
     */
    public void restartBookies(ServerConfiguration newConf) throws Exception {
        // shut down bookie server
        for (BookieServer server : bs) {
            server.shutdown();
            stopAutoRecoveryService(server);
        }
        bs.clear();
        Thread.sleep(1000);
        // restart them to ensure we can't

        List<ServerConfiguration> bsConfsCopy = new ArrayList<ServerConfiguration>(bsConfs);
        bsConfs.clear();
        for (ServerConfiguration conf : bsConfsCopy) {
            if (null != newConf) {
                conf.loadConf(newConf);
            }
            startBookie(conf);
        }
    }

    /**
     * Helper method to startup a new bookie server with the indicated port number. Also, starts the auto recovery
     * process, if the isAutoRecoveryEnabled is set true.
     *
     * @throws IOException
     */
    public int startNewBookie() throws Exception {
        ServerConfiguration conf = newServerConfiguration();
        startBookie(conf);

        return conf.getBookiePort();
    }

    /**
     * Helper method to startup a bookie server using a configuration object. Also, starts the auto recovery process if
     * isAutoRecoveryEnabled is true.
     *
     * @param conf
     *            Server Configuration Object
     *
     */
    protected BookieServer startBookie(ServerConfiguration conf, String ledgerRootPath) throws Exception {
        BookieServer server = new BookieServer(conf);
        bsConfs.add(conf);
        bs.add(server);

        server.start();

        if (bkc == null) {
            bkc = new BookKeeperTestClient(baseClientConf);
        }

        int port = conf.getBookiePort();
        while (bkc.getZkHandle()
            .exists(ledgerRootPath + "/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port,
                false) == null) {
            Thread.sleep(500);
        }

        bkc.readBookiesBlocking();
        LOG.info("New bookie on port " + port + " has been created.");

        return server;
    }

    protected BookieServer startBookie(ServerConfiguration conf) throws Exception {
        return startBookie(conf, "/ledgers");
    }

    /**
     * Start a bookie with the given bookie instance. Also, starts the auto recovery for this bookie, if
     * isAutoRecoveryEnabled is true.
     */
    protected BookieServer startBookie(ServerConfiguration conf, final Bookie b) throws Exception {
        BookieServer server = new BookieServer(conf) {
            @Override
            protected Bookie newBookie(ServerConfiguration conf, ByteBufAllocator allocator, Supplier<BookieServiceInfo> bookieServiceInfoProvider)
                    throws IOException, KeeperException, InterruptedException, BookieException {
                return b;
            }
        };
        server.start();

        int port = conf.getBookiePort();
        while (bkc.getZkHandle().exists(
                "/ledgers/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port, false) == null) {
            Thread.sleep(500);
        }

        bkc.readBookiesBlocking();
        LOG.info("New bookie on port " + port + " has been created.");
        return server;
    }

    public void setMetastoreImplClass(AbstractConfiguration conf) {
        conf.setMetastoreImplClass(InMemoryMetaStore.class.getName());
    }

    /**
     * Flags used to enable/disable the auto recovery process. If it is enabled, starting the bookie server will starts
     * the auto recovery process for that bookie. Also, stopping bookie will stops the respective auto recovery process.
     *
     * @param isAutoRecoveryEnabled
     *            Value true will enable the auto recovery process. Value false will disable the auto recovery process
     */
    public void setAutoRecoveryEnabled(boolean isAutoRecoveryEnabled) {
        this.isAutoRecoveryEnabled = isAutoRecoveryEnabled;
    }

    /**
     * Flag used to check whether auto recovery process is enabled/disabled. By default the flag is false.
     *
     * @return true, if the auto recovery is enabled. Otherwise return false.
     */
    public boolean isAutoRecoveryEnabled() {
        return isAutoRecoveryEnabled;
    }

    private void stopAutoRecoveryService(BookieServer toRemove) throws Exception {
        AutoRecoveryMain autoRecoveryMain = autoRecoveryProcesses.remove(toRemove);
        if (null != autoRecoveryMain && isAutoRecoveryEnabled()) {
            autoRecoveryMain.shutdown();
            LOG.debug("Shutdown auto recovery for bookieserver:" + toRemove.getLocalAddress());
        }
    }

    /**
     * Will stops all the auto recovery processes for the bookie cluster, if isAutoRecoveryEnabled is true.
     */
    public void stopReplicationService() throws Exception {
        if (false == isAutoRecoveryEnabled()) {
            return;
        }
        for (Entry<BookieServer, AutoRecoveryMain> autoRecoveryProcess : autoRecoveryProcesses.entrySet()) {
            autoRecoveryProcess.getValue().shutdown();
            LOG.debug("Shutdown Auditor Recovery for the bookie:" + autoRecoveryProcess.getKey().getLocalAddress());
        }
    }
}
