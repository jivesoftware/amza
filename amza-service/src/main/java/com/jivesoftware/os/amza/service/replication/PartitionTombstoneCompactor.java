package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.StripingLocksProvider;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class PartitionTombstoneCompactor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private ScheduledExecutorService scheduledThreadPool;

    private final AmzaStats amzaStats;
    private final IndexedWALStorageProvider indexedWALStorageProvider;
    private final PartitionCreator partitionCreator;
    private final PartitionIndex partitionIndex;
    private final StorageVersionProvider storageVersionProvider;
    private final long checkIfTombstoneCompactionIsNeededIntervalInMillis;
    private final long rebalanceableEveryNMillis;
    private final int numberOfStripes;
    private final long[] rebalanceableAfterTimestamp;
    private final StripingLocksProvider<PartitionName> locksProvider = new StripingLocksProvider<>(1024);

    public PartitionTombstoneCompactor(AmzaStats amzaStats,
        IndexedWALStorageProvider indexedWALStorageProvider,
        PartitionCreator partitionCreator,
        PartitionIndex partitionIndex,
        StorageVersionProvider storageVersionProvider,
        long checkIfCompactionIsNeededIntervalInMillis,
        long rebalanceableEveryNMillis,
        int numberOfStripes) {

        this.amzaStats = amzaStats;
        this.indexedWALStorageProvider = indexedWALStorageProvider;
        this.partitionCreator = partitionCreator;
        this.partitionIndex = partitionIndex;
        this.storageVersionProvider = storageVersionProvider;
        this.checkIfTombstoneCompactionIsNeededIntervalInMillis = checkIfCompactionIsNeededIntervalInMillis;
        this.rebalanceableEveryNMillis = rebalanceableEveryNMillis;
        this.numberOfStripes = numberOfStripes;
        this.rebalanceableAfterTimestamp = new long[numberOfStripes];
    }

    public void start() throws Exception {

        final int silenceBackToBackErrors = 100;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("partition-tombstone-compactor-%d").build();
        scheduledThreadPool = Executors.newScheduledThreadPool(numberOfStripes, threadFactory);
        for (int i = 0; i < numberOfStripes; i++) {
            int stripe = i;
            int[] failedToCompact = { 0 };
            scheduledThreadPool.scheduleWithFixedDelay(() -> {
                try {
                    failedToCompact[0] = 0;
                    compactTombstone(false, stripe);
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

    public void compactTombstone(boolean force, int compactStripe) throws Exception {

        int[] rebalanced = new int[1];
        partitionIndex.streamActivePartitions((versionedPartitionName) -> {
            PartitionName partitionName = versionedPartitionName.getPartitionName();
            synchronized (locksProvider.lock(partitionName, 123)) {
                storageVersionProvider.tx(partitionName,
                    null,
                    (deltaIndex, stripeIndex, storageVersion) -> {
                        if (storageVersion != null
                            && stripeIndex != -1
                            && storageVersion.partitionVersion == versionedPartitionName.getPartitionVersion()
                            && (compactStripe == -1 || stripeIndex == compactStripe)) {

                            PartitionStore partitionStore = partitionCreator.get(versionedPartitionName, stripeIndex);
                            if (partitionStore == null) {
                                return null;
                            }

                            boolean forced = force;
                            int compactToStripe = stripeIndex;
                            File fromBaseKey = indexedWALStorageProvider.baseKey(versionedPartitionName, stripeIndex);
                            File toBaseKey = fromBaseKey;

                            int rebalanceToStripe = -1;
                            long disposalVersion = -1;
                            if (!partitionName.isSystemPartition()) {
                                disposalVersion = partitionCreator.getPartitionDisposal(versionedPartitionName.getPartitionName());

                                if (force || rebalancingIsActive()) {
                                    rebalanceToStripe = indexedWALStorageProvider.rebalanceToStripe(versionedPartitionName,
                                        stripeIndex,
                                        partitionStore.getProperties());
                                    if (rebalanceToStripe > -1) {
                                        forced = true;
                                        compactToStripe = rebalanceToStripe;
                                        toBaseKey = indexedWALStorageProvider.baseKey(versionedPartitionName, compactToStripe);
                                        LOG.info("Rebalancing by compacting {} from {}:{} to {}:{}",
                                            partitionName,
                                            stripeIndex,
                                            fromBaseKey,
                                            compactToStripe,
                                            toBaseKey);
                                    }
                                }
                            }
                            int effectivelyFinalRebalanceToStripe = rebalanceToStripe;
                            partitionStore.compactTombstone(amzaStats,
                                forced,
                                fromBaseKey,
                                toBaseKey,
                                compactToStripe,
                                disposalVersion,
                                (transitionToCompactedTx) -> {
                                    return storageVersionProvider.replaceOneWithAll(partitionName,
                                        () -> {
                                            return transitionToCompactedTx.tx(() -> {
                                                if (effectivelyFinalRebalanceToStripe != -1) {
                                                    rebalanced[0]++;
                                                    storageVersionProvider.transitionStripe(versionedPartitionName,
                                                        storageVersion,
                                                        effectivelyFinalRebalanceToStripe);

                                                    LOG.info("Rebalancing transitioned {} to {}", partitionName, effectivelyFinalRebalanceToStripe);
                                                }
                                                return null;
                                            });
                                        });
                                });

                        }
                        return null;
                    });
                return true;
            }
        });

        if (compactStripe != -1 && rebalanced[0] == 0 && System.currentTimeMillis() > rebalanceableAfterTimestamp[compactStripe]) {
            rebalanceableAfterTimestamp[compactStripe] = System.currentTimeMillis() + rebalanceableEveryNMillis;
            LOG.info("Rebalancing for stripe {} has been paused until {}", compactStripe, rebalanceableAfterTimestamp[compactStripe]);
        }
    }

    private boolean rebalancingIsActive() {
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < rebalanceableAfterTimestamp.length; i++) {
            if (timestamp > rebalanceableAfterTimestamp[i]) {
                return true;
            }
        }
        return false;
    }

}
