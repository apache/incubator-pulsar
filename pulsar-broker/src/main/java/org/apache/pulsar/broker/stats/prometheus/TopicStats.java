package org.apache.pulsar.broker.stats.prometheus;

import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.utils.SimpleTextOutputStream;

import java.util.HashMap;
import java.util.Map;

class TopicStats {

    private static final String TOPIC_REGEX = ".*://";

    int subscriptionsCount;
    int producersCount;
    int consumersCount;
    double rateIn;
    double rateOut;
    double throughputIn;
    double throughputOut;

    long storageSize;
    public long msgBacklog;

    StatsBuckets storageWriteLatencyBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC);
    StatsBuckets entrySizeBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES);
    double storageWriteRate;
    double storageReadRate;

    Map<String, AggregatedReplicationStats> replicationStats = new HashMap<>();

    public void reset() {
        subscriptionsCount = 0;
        producersCount = 0;
        consumersCount = 0;
        rateIn = 0;
        rateOut = 0;
        throughputIn = 0;
        throughputOut = 0;

        storageSize = 0;
        msgBacklog = 0;
        storageWriteRate = 0;
        storageReadRate = 0;

        replicationStats.clear();
        storageWriteLatencyBuckets.reset();
        entrySizeBuckets.reset();
    }

    static void printNamespaceStats(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
        TopicStats stats) {

        topic = cleanTopicName(topic);

        metric(stream, cluster, namespace, topic,"pulsar_subscriptions_count", stats.subscriptionsCount);
        metric(stream, cluster, namespace, topic,"pulsar_producers_count", stats.producersCount);
        metric(stream, cluster, namespace, topic,"pulsar_consumers_count", stats.consumersCount);

        metric(stream, cluster, namespace, topic,"pulsar_rate_in", stats.rateIn);
        metric(stream, cluster, namespace, topic,"pulsar_rate_out", stats.rateOut);
        metric(stream, cluster, namespace, topic,"pulsar_throughput_in", stats.throughputIn);
        metric(stream, cluster, namespace, topic,"pulsar_throughput_out", stats.throughputOut);

        metric(stream, cluster, namespace, topic,"pulsar_storage_size", stats.storageSize);
        metric(stream, cluster, namespace, topic,"pulsar_msg_backlog", stats.msgBacklog);
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
        String name, double value) {
        stream.write(name).write("{cluster=\"").write(cluster).write("\", namespace=\"").write(namespace)
                .write("\", topic=\"").write(topic).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static String cleanTopicName(String topic) {
        return topic.replaceFirst(TOPIC_REGEX, "");
    }
}
