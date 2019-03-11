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
package org.apache.pulsar.io.redis;

import lombok.extern.slf4j.Slf4j;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.util.Architecture;
import redis.embedded.util.OS;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public final class EmbeddedRedisUtils {

    private final Path dbPath;
    private final RedisServer redisServer;

    public EmbeddedRedisUtils(String testId) {
        dbPath = Paths.get(testId + "/redis");
        String execFile = "redis-server-2.8.19";
        // JarLoader
        String executable = getRuntimePath() + "!/" + execFile;
        RedisExecProvider customProvider = RedisExecProvider
            .defaultProvider()
            .override(OS.UNIX, Architecture.x86_64, executable);
        redisServer = RedisServer.builder()
            .redisExecProvider(customProvider)
            .port(6379)
            .slaveOf("localhost", 6378)
            .setting("daemonize no")
            .setting("appendonly no")
            .setting("maxheap 128M")
            .build();
    }

    public void setUp() throws IOException {
        redisServer.start();
        Files.deleteIfExists(dbPath);
    }

    public void tearDown() throws IOException {
        redisServer.stop();
        Files.deleteIfExists(dbPath);
    }

    private static String getRuntimePath() {
        String classPath = EmbeddedRedisUtils.class.getName().replaceAll("\\.", "/") + ".class";
        URL resource = EmbeddedRedisUtils.class.getClassLoader().getResource(classPath);
        if (resource == null) {
            return null;
        }
        String urlString = resource.toString();
        int insidePathIndex = urlString.indexOf('!');
        boolean isInJar = insidePathIndex > -1;
        if (isInJar) {
            urlString = urlString.substring(urlString.indexOf("file:"), insidePathIndex);
            return urlString;
        }
        return urlString.substring(urlString.indexOf("file:"), urlString.length() - classPath.length());
    }

}
