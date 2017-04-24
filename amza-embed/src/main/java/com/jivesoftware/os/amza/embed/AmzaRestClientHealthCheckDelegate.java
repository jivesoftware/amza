package com.jivesoftware.os.amza.embed;

import com.jivesoftware.os.amza.api.PartitionClient.KeyValueFilter;
import com.jivesoftware.os.amza.api.RingPartitionProperties;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.service.Partition.ScanRange;
import com.jivesoftware.os.amza.service.replication.http.AmzaRestClient;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthTimer;
import com.jivesoftware.os.routing.bird.health.api.TimerHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.TimerHealthChecker;
import java.io.IOException;
import java.util.List;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class AmzaRestClientHealthCheckDelegate implements AmzaRestClient {

    private final AmzaRestClient client;

    public AmzaRestClientHealthCheckDelegate(AmzaRestClient client) {
        this.client = client;
    }

    @Override
    public RingPartitionProperties getProperties(PartitionName partitionName) throws Exception {
        return client.getProperties(partitionName);
    }

    public static interface CommitLatency extends TimerHealthCheckConfig {

        @StringDefault("client>commit>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to commit.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer commitLatency = HealthFactory.getHealthTimer(CommitLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public StateMessageCause commit(PartitionName partitionName, Consistency consistency, boolean checkLeader, long partitionAwaitOnlineTimeoutMillis,
        IReadable read) throws Exception {
        try {
            commitLatency.startTimer();
            return client.commit(partitionName, consistency, checkLeader, partitionAwaitOnlineTimeoutMillis, read);
        } finally {
            commitLatency.stopTimer("Commit", "Check cluster health.");
        }
    }

    public static interface ConfigRequestLatency extends TimerHealthCheckConfig {

        @StringDefault("client>config>request>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to config a partition.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer configRequestLatency = HealthFactory.getHealthTimer(ConfigRequestLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public RingTopology configPartition(PartitionName partitionName, PartitionProperties partitionProperties, int ringSize) throws Exception {
        try {
            configRequestLatency.startTimer();
            return client.configPartition(partitionName, partitionProperties, ringSize);
        } finally {
            configRequestLatency.stopTimer("Config request", "Check cluster health.");
        }
    }

    public static interface ConfigResponseLatency extends TimerHealthCheckConfig {

        @StringDefault("client>config>response>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to flush the response for a config request.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer configResponseLatency = HealthFactory.getHealthTimer(ConfigResponseLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public void configPartition(RingTopology ring, IWriteable writeable) throws Exception {
        try {
            configResponseLatency.startTimer();
            client.configPartition(ring, writeable);
        } finally {
            configResponseLatency.stopTimer("Config response", "Check cluster health.");
        }
    }

    public static interface EnsureLatency extends TimerHealthCheckConfig {

        @StringDefault("client>ensure>latency")
        @Override
        String getName();

        @StringDefault("How long its taking ensure a partition.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer ensureLatency = HealthFactory.getHealthTimer(EnsureLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public void ensurePartition(PartitionName partitionName, long waitForLeaderElection) throws Exception {
        try {
            ensureLatency.startTimer();
            client.ensurePartition(partitionName, waitForLeaderElection);
        } finally {
            ensureLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

    public static interface GetResponseLatency extends TimerHealthCheckConfig {

        @StringDefault("client>get>response>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to get.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer getResponseLatency = HealthFactory.getHealthTimer(GetResponseLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public void get(PartitionName partitionName, Consistency consistency, IReadable in, IWriteable out) throws Exception {
        try {
            getResponseLatency.startTimer();
            client.get(partitionName, consistency, in, out);
        } finally {
            getResponseLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

    public static interface GetOffsetResponseLatency extends TimerHealthCheckConfig {

        @StringDefault("client>getOffset>response>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to getOffset.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer getOffsetResponseLatency = HealthFactory.getHealthTimer(GetOffsetResponseLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public void getOffset(PartitionName partitionName, Consistency consistency, IReadable in, IWriteable out) throws Exception {
        try {
            getOffsetResponseLatency.startTimer();
            client.getOffset(partitionName, consistency, in, out);
        } finally {
            getOffsetResponseLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

    public static interface RingRequestLatency extends TimerHealthCheckConfig {

        @StringDefault("client>ring>request>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to request a ring.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer ringRequestLatency = HealthFactory.getHealthTimer(RingRequestLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public RingLeader ring(PartitionName partitionName) throws Exception {
        try {
            ringRequestLatency.startTimer();
            return client.ring(partitionName);
        } finally {
            ringRequestLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

    public static interface RingLeaderRequestLatency extends TimerHealthCheckConfig {

        @StringDefault("client>ringLeader>request>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to request a ringLeader.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer ringLeaderRequestLatency = HealthFactory.getHealthTimer(RingLeaderRequestLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public RingLeader ringLeader(PartitionName partitionName, long waitForLeaderElection) throws Exception {
        try {
            ringLeaderRequestLatency.startTimer();
            return client.ringLeader(partitionName, waitForLeaderElection);
        } finally {
            ringLeaderRequestLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

    public static interface RingResponseLatency extends TimerHealthCheckConfig {

        @StringDefault("client>ring>response>latency")
        @Override
        String getName();

        @StringDefault("How long its taking return the ring.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer ringResponseLatency = HealthFactory.getHealthTimer(RingResponseLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public void ring(RingLeader ringLeader, IWriteable writeable) throws IOException {
        try {
            ringResponseLatency.startTimer();
            client.ring(ringLeader, writeable);
        } finally {
            ringResponseLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

    public static interface ScanResponseLatency extends TimerHealthCheckConfig {

        @StringDefault("client>scan>response>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to scan.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer scanResponseLatency = HealthFactory.getHealthTimer(ScanResponseLatency.class, TimerHealthChecker.FACTORY);

    public static interface ScanKeysResponseLatency extends TimerHealthCheckConfig {

        @StringDefault("client>scanKeys>response>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to scanKeys.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer scanKeysResponseLatency = HealthFactory.getHealthTimer(ScanKeysResponseLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public void scan(PartitionName partitionName, List<ScanRange> ranges, KeyValueFilter filter, IWriteable out, boolean hydrateValues) throws Exception {
        HealthTimer timer = hydrateValues ? scanResponseLatency : scanKeysResponseLatency;
        try {
            timer.startTimer();
            client.scan(partitionName, ranges, filter, out, hydrateValues);
        } finally {
            timer.stopTimer("Ensure", "Check cluster health.");
        }
    }

    public static interface StatusLatency extends TimerHealthCheckConfig {

        @StringDefault("client>status>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to determine status.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer statusLatency = HealthFactory.getHealthTimer(StatusLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public StateMessageCause status(PartitionName partitionName, Consistency consistency, boolean checkLeader, long partitionAwaitOnlineTimeoutMillis) {
        try {
            statusLatency.startTimer();
            return client.status(partitionName, consistency, checkLeader, partitionAwaitOnlineTimeoutMillis);
        } finally {
            statusLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

    public static interface TakeFromLatency extends TimerHealthCheckConfig {

        @StringDefault("client>takeFrom>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to take.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer takeFromLatency = HealthFactory.getHealthTimer(TakeFromLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public void takeFromTransactionId(PartitionName partitionName, int limit, IReadable in, IWriteable out) throws Exception {
        try {
            takeFromLatency.startTimer();
            client.takeFromTransactionId(partitionName, limit, in, out);
        } finally {
            takeFromLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

    public static interface TakeFromWithPrefixLatency extends TimerHealthCheckConfig {

        @StringDefault("client>takeFromWithPrefix>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to take.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer takeFromWithPrefixLatency = HealthFactory.getHealthTimer(TakeFromLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public void takePrefixFromTransactionId(PartitionName partitionName, int limit, IReadable in, IWriteable out) throws Exception {
        try {
            takeFromWithPrefixLatency.startTimer();
            client.takePrefixFromTransactionId(partitionName, limit, in, out);
        } finally {
            takeFromWithPrefixLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }


    public static interface ApproximateCountLatency extends TimerHealthCheckConfig {

        @StringDefault("client>approximateCount>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to get approximate count.")
        @Override
        String getDescription();

        @DoubleDefault(3600000d)
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer approximateCountLatency = HealthFactory.getHealthTimer(TakeFromLatency.class, TimerHealthChecker.FACTORY);

    @Override
    public long approximateCount(PartitionName partitionName) throws Exception {
        try {
            approximateCountLatency.startTimer();
            return client.approximateCount(partitionName);
        } finally {
            approximateCountLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

}
