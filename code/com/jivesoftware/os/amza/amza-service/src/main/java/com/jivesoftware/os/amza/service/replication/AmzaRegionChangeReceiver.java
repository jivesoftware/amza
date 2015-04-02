package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.service.storage.WALs;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jonathan.colt
 */
public class AmzaRegionChangeReceiver {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private ScheduledExecutorService applyThreadPool;
    private ScheduledExecutorService compactThreadPool;
    private final AmzaStats amzaStats;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final RegionProvider regionProvider;
    private final Map<RegionName, Long> highwaterMarks = new ConcurrentHashMap<>();
    private final WALs receivedWAL;
    private final long applyReplicasIntervalInMillis;
    private final int numberOfApplierThreads;
    private final Object[] receivedLocks;

    public AmzaRegionChangeReceiver(AmzaStats amzaStats,
        RowMarshaller<byte[]> rowMarshaller,
        RegionProvider regionProvider,
        WALs receivedUpdatesWAL,
        long applyReplicasIntervalInMillis,
        int numberOfApplierThreads) {
        this.amzaStats = amzaStats;
        this.rowMarshaller = rowMarshaller;
        this.regionProvider = regionProvider;
        this.receivedWAL = receivedUpdatesWAL;
        this.applyReplicasIntervalInMillis = applyReplicasIntervalInMillis;
        this.numberOfApplierThreads = numberOfApplierThreads;
        this.receivedLocks = new Object[numberOfApplierThreads];
        for (int i = 0; i < receivedLocks.length; i++) {
            receivedLocks[i] = new Object();
        }
    }

    public void receiveChanges(RegionName regionName, WALScanable changes) throws Exception {
        RowsChanged changed = receivedWAL.get(regionName).update(WALStorageUpdateMode.noReplication, changes);
        amzaStats.received(regionName, changed.getApply().size(), changed.getOldestRowTxId());

        Object lock = receivedLocks[Math.abs(regionName.hashCode()) % numberOfApplierThreads];
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    synchronized public void start() {
        if (applyThreadPool == null) {
            applyThreadPool = Executors.newScheduledThreadPool(numberOfApplierThreads, new ThreadFactoryBuilder().setNameFormat("applyReceivedChanges-%d")
                .build());
            for (int i = 0; i < numberOfApplierThreads; i++) {
                final int stripe = i;
                applyThreadPool.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            applyReceivedChanges(stripe);
                        } catch (Throwable x) {
                            LOG.warn("Shouldn't have gotten here. Implementors please catch your exceptions.", x);
                        }
                    }
                }, applyReplicasIntervalInMillis, applyReplicasIntervalInMillis, TimeUnit.MILLISECONDS);
            }
        }
        if (compactThreadPool == null) {
            compactThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("compactReceivedChanges-%d").build());
            compactThreadPool.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {
                    try {
                        compactReceivedChanges();
                    } catch (Throwable x) {
                        LOG.error("Failing to compact.", x);
                    }
                }
            }, 1, 1, TimeUnit.MINUTES);
        }
    }

    synchronized public void stop() {
        if (applyThreadPool != null) {
            this.applyThreadPool.shutdownNow();
            this.applyThreadPool = null;
        }
        if (compactThreadPool != null) {
            this.compactThreadPool.shutdownNow();
            this.compactThreadPool = null;
        }
    }

    private void applyReceivedChanges(int stripe) throws Exception {

        while (true) {
            boolean appliedChanges = false;
            for (RegionName regionName : regionProvider.getActiveRegions()) {
                WALStorage received = receivedWAL.get(regionName);
                if (Math.abs(regionName.hashCode()) % numberOfApplierThreads == stripe) {
                    RegionStore regionStore = regionProvider.getRegionStore(regionName);
                    if (regionStore != null) {
                        Long highWatermark = highwaterMarks.get(regionName);
                        HighwaterInterceptor highwaterInterceptor = new HighwaterInterceptor(highWatermark, received);
                        WALScanBatchinator batchinator = new WALScanBatchinator(amzaStats, rowMarshaller, regionName, regionStore);
                        highwaterInterceptor.rowScan(batchinator);
                        if (batchinator.flush()) {
                            highwaterMarks.put(regionName, highwaterInterceptor.getHighwater());
                            appliedChanges = true;
                        }
                    }
                }
            }
            if (!appliedChanges) {
                synchronized (receivedLocks[stripe]) {
                    receivedLocks[stripe].wait();
                }
            }
        }
    }

    private void compactReceivedChanges() throws Exception {
        for (RegionName regionName : regionProvider.getActiveRegions()) {
            Long highWatermark = highwaterMarks.get(regionName);
            if (highWatermark != null) {
                WALStorage regionWAL = receivedWAL.get(regionName);
                amzaStats.beginCompaction("Compacting Received:" + regionName);
                try {
                    regionWAL.compactTombstone(highWatermark, highWatermark);
                } finally {
                    amzaStats.endCompaction("Compacting Received:" + regionName);
                }
            }
        }
    }
}
