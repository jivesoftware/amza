package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class PartitionTombstoneCompactor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private ScheduledExecutorService scheduledThreadPool;

    private final PartitionIndex partitionIndex;
    private final PartitionStripeFunction partitionStripeFunction;
    private final long checkIfTombstoneCompactionIsNeededIntervalInMillis;
    private final int numberOfCompactorThreads;
    private final StripingLocksProvider<VersionedPartitionName> locksProvider = new StripingLocksProvider<>(1024);

    public PartitionTombstoneCompactor(PartitionIndex partitionIndex,
        PartitionStripeFunction partitionStripeFunction,
        long checkIfCompactionIsNeededIntervalInMillis,
        int numberOfCompactorThreads) {

        this.partitionIndex = partitionIndex;
        this.partitionStripeFunction = partitionStripeFunction;
        this.checkIfTombstoneCompactionIsNeededIntervalInMillis = checkIfCompactionIsNeededIntervalInMillis;
        this.numberOfCompactorThreads = numberOfCompactorThreads;
    }

    public void start() throws Exception {

        final int silenceBackToBackErrors = 100;
        scheduledThreadPool = Executors.newScheduledThreadPool(numberOfCompactorThreads,
            new ThreadFactoryBuilder().setNameFormat("partition-tombstone-compactor-%d").build());
        for (int i = 0; i < numberOfCompactorThreads; i++) {
            int stripe = i;
            int[] failedToCompact = { 0 };
            scheduledThreadPool.scheduleWithFixedDelay(() -> {
                try {
                    failedToCompact[0] = 0;
                    compactTombstone(stripe, false);
                } catch (Exception x) {
                    LOG.debug("Failing to compact tombstones.", x);
                    if (failedToCompact[0] % silenceBackToBackErrors == 0) {
                        failedToCompact[0]++;
                        LOG.error("Failing to compact tombstones.");
                    }
                }
            }, checkIfTombstoneCompactionIsNeededIntervalInMillis, checkIfTombstoneCompactionIsNeededIntervalInMillis, TimeUnit.MILLISECONDS);
        }
    }

    public void stop() throws Exception {
        this.scheduledThreadPool.shutdownNow();
        this.scheduledThreadPool = null;
    }

    public void compactTombstone(int stripe, boolean force) throws Exception {

        for (VersionedPartitionName versionedPartitionName : partitionIndex.getMemberPartitions()) {
            if (stripe == -1 || partitionStripeFunction.stripe(versionedPartitionName.getPartitionName()) == stripe) {
                partitionIndex.get(versionedPartitionName).compactTombstone(force);
            }
        }
    }

}
