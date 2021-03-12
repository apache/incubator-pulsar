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
package org.apache.pulsar.broker.stats;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.common.stats.Metrics;

/**
 */
public class BrokerOperabilityMetrics {
    private final List<Metrics> metricsList;
    private final String localCluster;
    private final DimensionStats topicLoadStats;
    private final DimensionStats zkWriteLatencyStats;
    private final DimensionStats zkReadLatencyStats;
    private final String brokerName;
    private final AtomicLong connectionTotalCreatedCount;
    private final AtomicLong connectionCreateSuccessCount;
    private final AtomicLong connectionCreateFailCount;
    private final AtomicLong connectionTotalClosedCount;
    private final AtomicLong connectionActive;

    public BrokerOperabilityMetrics(String localCluster, String brokerName) {
        this.metricsList = new ArrayList<>();
        this.localCluster = localCluster;
        this.topicLoadStats = new DimensionStats("topic_load_times", 60);
        this.zkWriteLatencyStats = new DimensionStats("zk_write_latency", 60);
        this.zkReadLatencyStats = new DimensionStats("zk_read_latency", 60);
        this.brokerName = brokerName;
        this.connectionTotalCreatedCount = new AtomicLong();
        this.connectionCreateSuccessCount = new AtomicLong();
        this.connectionCreateFailCount = new AtomicLong();
        this.connectionTotalClosedCount = new AtomicLong();
        this.connectionActive = new AtomicLong();
    }

    public List<Metrics> getMetrics() {
        generate();
        return metricsList;
    }

    private void generate() {
        metricsList.add(getTopicLoadMetrics());
        metricsList.add(getZkWriteLatencyMetrics());
        metricsList.add(getZkReadLatencyMetrics());
        metricsList.add(getConnectionMetrics());
    }

    public Metrics generateConnectionMetrics() {
        return getConnectionMetrics();
    }

    Metrics getConnectionMetrics() {
        Metrics rMetrics = Metrics.create(getDimensionMap("broker_connection"));
        rMetrics.put("brk_connection_created_total_count", connectionTotalCreatedCount.get());
        rMetrics.put("brk_connection_create_success_count", connectionCreateSuccessCount.get());
        rMetrics.put("brk_connection_create_fail_count", connectionCreateFailCount.get());
        rMetrics.put("brk_connection_closed_total_count", connectionTotalClosedCount.get());
        rMetrics.put("brk_active_connections", connectionActive.get());
        return rMetrics;
    }

    Map<String, String> getDimensionMap(String metricsName) {
        Map<String, String> dimensionMap = Maps.newHashMap();
        dimensionMap.put("broker", brokerName);
        dimensionMap.put("cluster", localCluster);
        dimensionMap.put("metric", metricsName);
        return dimensionMap;
    }

    Metrics getTopicLoadMetrics() {
        return getDimensionMetrics("topic_load_times", "topic_load", topicLoadStats);
    }

    Metrics getZkWriteLatencyMetrics() {
        return getDimensionMetrics("zk_write_latency", "zk_write", zkWriteLatencyStats);
    }

    Metrics getZkReadLatencyMetrics() {
        return getDimensionMetrics("zk_read_latency", "zk_read", zkReadLatencyStats);
    }

    Metrics getDimensionMetrics(String metricsName, String dimensionName, DimensionStats stats) {
        Metrics dMetrics = Metrics.create(getDimensionMap(metricsName));

        dMetrics.put("brk_" + dimensionName + "_time_mean_ms", stats.getMeanDimension());
        dMetrics.put("brk_" + dimensionName + "_time_median_ms", stats.getMedianDimension());
        dMetrics.put("brk_" + dimensionName + "_time_75percentile_ms", stats.getDimension75());
        dMetrics.put("brk_" + dimensionName + "_time_95percentile_ms", stats.getDimension95());
        dMetrics.put("brk_" + dimensionName + "_time_99_percentile_ms", stats.getDimension99());
        dMetrics.put("brk_" + dimensionName + "_time_99_9_percentile_ms", stats.getDimension999());
        dMetrics.put("brk_" + dimensionName + "_time_99_99_percentile_ms", stats.getDimension9999());
        dMetrics.put("brk_" + dimensionName + "_rate_s", stats.getDimensionCount());

        return dMetrics;
    }

    public void reset() {
        metricsList.clear();
        topicLoadStats.reset();
        zkWriteLatencyStats.reset();
        zkReadLatencyStats.reset();
    }

    public void recordTopicLoadTimeValue(long topicLoadLatencyMs) {
        topicLoadStats.recordDimensionTimeValue(topicLoadLatencyMs, TimeUnit.MILLISECONDS);
    }

    public void recordZkWriteLatencyTimeValue(long topicLoadLatencyMs) {
        zkWriteLatencyStats.recordDimensionTimeValue(topicLoadLatencyMs, TimeUnit.MILLISECONDS);
    }

    public void recordZkReadLatencyTimeValue(long topicLoadLatencyMs) {
        zkReadLatencyStats.recordDimensionTimeValue(topicLoadLatencyMs, TimeUnit.MILLISECONDS);
    }

    public void recordConnectionCreate() {
        this.connectionTotalCreatedCount.incrementAndGet();
        this.connectionActive.incrementAndGet();
    }

    public void recordConnectionClose() {
        this.connectionTotalClosedCount.incrementAndGet();
        this.connectionActive.decrementAndGet();
    }

    public void recordConnectionCreateSuccess() {
        this.connectionCreateSuccessCount.incrementAndGet();
    }

    public void recordConnectionCreateFail() {
        this.connectionCreateFailCount.incrementAndGet();
    }
}
