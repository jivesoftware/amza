package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class AmzaRegionCompactor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private ScheduledExecutorService scheduledThreadPool;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final AmzaStats amzaStats;
    private final RegionProvider regionProvider;
    private final long checkIfCompactionIsNeededIntervalInMillis;
    private final long removeTombstonedOlderThanNMillis;
    private final int numberOfCompactorThreads;

    public AmzaRegionCompactor(AmzaStats amzaStats,
        RegionProvider regionProvider,
        TimestampedOrderIdProvider orderIdProvider,
        long checkIfCompactionIsNeededIntervalInMillis,
        final long removeTombstonedOlderThanNMillis,
        int numberOfCompactorThreads) {

        this.amzaStats = amzaStats;
        this.regionProvider = regionProvider;
        this.orderIdProvider = orderIdProvider;
        this.checkIfCompactionIsNeededIntervalInMillis = checkIfCompactionIsNeededIntervalInMillis;
        this.removeTombstonedOlderThanNMillis = removeTombstonedOlderThanNMillis;
        this.numberOfCompactorThreads = numberOfCompactorThreads;
    }

    synchronized public void start() throws Exception {

        final int silenceBackToBackErrors = 100;
        if (scheduledThreadPool == null) {
            scheduledThreadPool = Executors.newScheduledThreadPool(numberOfCompactorThreads,
                new ThreadFactoryBuilder().setNameFormat("region-compactor-%d").build());
            for (int i = 0; i < numberOfCompactorThreads; i++) {
                final int stripe = i;
                scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
                    int failedToCompact = 0;

                    @Override
                    public void run() {
                        try {
                            failedToCompact = 0;
                            long removeIfOlderThanTimestmapId = orderIdProvider.getApproximateId(System.currentTimeMillis() - removeTombstonedOlderThanNMillis);
                            compactTombstone(stripe, removeIfOlderThanTimestmapId);
                        } catch (Exception x) {
                            LOG.debug("Failing to compact tombstones.", x);
                            if (failedToCompact % silenceBackToBackErrors == 0) {
                                failedToCompact++;
                                LOG.error("Failing to compact tombstones.");
                            }
                        }
                    }
                }, checkIfCompactionIsNeededIntervalInMillis, checkIfCompactionIsNeededIntervalInMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    synchronized public void stop() throws Exception {
        if (scheduledThreadPool != null) {
            this.scheduledThreadPool.shutdownNow();
            this.scheduledThreadPool = null;
        }
    }

    private void compactTombstone(int stripe, long removeTombstonedOlderThanTimestampId) throws Exception {
        for (Map.Entry<RegionName, RegionStore> region : regionProvider.getAll()) {
            if (Math.abs(region.getKey().hashCode()) % numberOfCompactorThreads == stripe) {
                RegionStore regionStore = region.getValue();
                long ttlTimestampId = 0;
                RegionProperties regionProperties = regionProvider.getRegionProperties(region.getKey());
                if (regionProperties != null) {
                    long ttlInMillis = regionProperties.walStorageDescriptor.primaryIndexDescriptor.ttlInMillis;
                    if (ttlInMillis > 0) {
                        ttlTimestampId = orderIdProvider.getApproximateId(orderIdProvider.nextId(), -ttlInMillis);
                    }
                }
                try {
                    amzaStats.beginCompaction("Compacting Tombstones:" + region.getKey());
                    try {
                        regionStore.compactTombstone(removeTombstonedOlderThanTimestampId, ttlTimestampId);
                    } finally {
                        amzaStats.endCompaction("Compacting Tombstones:" + region.getKey());
                    }
                } catch (Exception x) {
                    LOG.warn("Failed to compact tombstones region:" + region.getKey(), x);
                }
            }
        }
    }

}
