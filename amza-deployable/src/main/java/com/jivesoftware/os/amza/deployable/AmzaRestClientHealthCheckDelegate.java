package com.jivesoftware.os.amza.deployable;

import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.ring.RingTopology;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaRestClient;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthTimer;
import com.jivesoftware.os.routing.bird.health.api.TimerHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.TimerHealthChecker;
import java.io.IOException;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 * @author jonathan.colt
 */
public class AmzaRestClientHealthCheckDelegate implements AmzaRestClient {

    private final AmzaRestClient client;

    public AmzaRestClientHealthCheckDelegate(AmzaRestClient client) {
        this.client = client;
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
    public RingLeader ring(PartitionName partitionName, long waitForLeaderElection) throws Exception {
        try {
            ringRequestLatency.startTimer();
            return client.ring(partitionName, waitForLeaderElection);
        } finally {
            ringRequestLatency.stopTimer("Ensure", "Check cluster health.");
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

    @Override
    public void scan(PartitionName partitionName, IReadable in, IWriteable out) throws Exception {
        try {
            scanResponseLatency.startTimer();
            client.scan(partitionName, in, out);
        } finally {
            scanResponseLatency.stopTimer("Ensure", "Check cluster health.");
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
    public void takeFromTransactionId(PartitionName partitionName, IReadable in, IWriteable out) throws Exception {
        try {
            takeFromLatency.startTimer();
            client.takeFromTransactionId(partitionName, in, out);
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
    public void takePrefixFromTransactionId(PartitionName partitionName, IReadable in, IWriteable out) throws Exception {
        try {
            takeFromWithPrefixLatency.startTimer();
            client.takePrefixFromTransactionId(partitionName, in, out);
        } finally {
            takeFromWithPrefixLatency.stopTimer("Ensure", "Check cluster health.");
        }
    }

}
