package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class PartitionTombstoneCompactor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private ScheduledExecutorService scheduledThreadPool;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final AmzaStats amzaStats;
    private final PartitionIndex partitionIndex;
    private final long checkIfTombstoneCompactionIsNeededIntervalInMillis;
    private final long removeTombstonedOlderThanNMillis;
    private final int numberOfCompactorThreads;
    private final Map<VersionedPartitionName, Boolean> compacting = new ConcurrentHashMap<>();

    public PartitionTombstoneCompactor(AmzaStats amzaStats,
        PartitionIndex partitionIndex,
        TimestampedOrderIdProvider orderIdProvider,
        long checkIfCompactionIsNeededIntervalInMillis,
        final long removeTombstonedOlderThanNMillis,
        int numberOfCompactorThreads) {

        this.amzaStats = amzaStats;
        this.partitionIndex = partitionIndex;
        this.orderIdProvider = orderIdProvider;
        this.checkIfTombstoneCompactionIsNeededIntervalInMillis = checkIfCompactionIsNeededIntervalInMillis;
        this.removeTombstonedOlderThanNMillis = removeTombstonedOlderThanNMillis;
        this.numberOfCompactorThreads = numberOfCompactorThreads;
    }

    synchronized public void start() throws Exception {

        final int silenceBackToBackErrors = 100;
        if (scheduledThreadPool == null) {
            scheduledThreadPool = Executors.newScheduledThreadPool(numberOfCompactorThreads,
                new ThreadFactoryBuilder().setNameFormat("partition-compactor-%d").build());
            for (int i = 0; i < numberOfCompactorThreads; i++) {
                final int stripe = i;
                scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
                    int failedToCompact = 0;

                    @Override
                    public void run() {
                        try {
                            failedToCompact = 0;
                            long removeIfOlderThanTimestmapId = removeIfOlderThanTimestmapId();
                            compactTombstone(stripe, numberOfCompactorThreads, removeIfOlderThanTimestmapId);
                        } catch (Exception x) {
                            LOG.debug("Failing to compact tombstones.", x);
                            if (failedToCompact % silenceBackToBackErrors == 0) {
                                failedToCompact++;
                                LOG.error("Failing to compact tombstones.");
                            }
                        }
                    }
                }, checkIfTombstoneCompactionIsNeededIntervalInMillis, checkIfTombstoneCompactionIsNeededIntervalInMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    synchronized public void stop() throws Exception {
        if (scheduledThreadPool != null) {
            this.scheduledThreadPool.shutdownNow();
            this.scheduledThreadPool = null;
        }
    }

    public long removeIfOlderThanTimestmapId() {
        return orderIdProvider.getApproximateId(System.currentTimeMillis() - removeTombstonedOlderThanNMillis);
    }

    public void compactTombstone(int stripe, int numberOfStripes, long removeTombstonedOlderThanTimestampId) throws Exception {

        for (VersionedPartitionName versionedPartitionName : partitionIndex.getAllPartitions()) {
            if (Math.abs(versionedPartitionName.getPartitionName().hashCode()) % numberOfStripes == stripe) {

                long ttlTimestampId = 0;
                PartitionProperties partitionProperties = partitionIndex.getProperties(versionedPartitionName.getPartitionName());
                if (partitionProperties != null) {
                    long ttlInMillis = partitionProperties.walStorageDescriptor.primaryIndexDescriptor.ttlInMillis;
                    if (ttlInMillis > 0) {
                        ttlTimestampId = orderIdProvider.getApproximateId(orderIdProvider.nextId(), -ttlInMillis);
                    }
                }

                if (compacting.computeIfAbsent(versionedPartitionName, (key) -> {
                    return true;
                }) == null) {
                    try {
                        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                        if (partitionStore.compactableTombstone(removeTombstonedOlderThanTimestampId, ttlTimestampId)) {
                            amzaStats.beginCompaction("Compacting Tombstones:" + versionedPartitionName);
                            try {
                                partitionStore.compactTombstone(removeTombstonedOlderThanTimestampId, ttlTimestampId);
                            } finally {
                                amzaStats.endCompaction("Compacting Tombstones:" + versionedPartitionName);
                            }
                        }
                    } catch (Exception x) {
                        LOG.warn("Failed to compact tombstones partition:" + versionedPartitionName, x);
                    } finally {
                        compacting.remove(versionedPartitionName);
                    }
                } else {
                    LOG.warn("Tried to compact tombostones for {} but there was already a compaction underway.", versionedPartitionName);
                }
            }
        }
    }

}
