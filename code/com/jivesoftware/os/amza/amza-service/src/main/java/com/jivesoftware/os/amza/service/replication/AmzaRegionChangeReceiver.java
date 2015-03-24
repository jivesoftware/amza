package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.service.storage.WALs;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
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
    private ScheduledExecutorService scheduledThreadPool;
    private final AmzaStats amzaStats;
    private final RegionProvider regionProvider;
    private final Map<RegionName, Long> highwaterMarks = new ConcurrentHashMap<>();
    private final WALs receivedWAL;
    private final long applyReplicasIntervalInMillis;

    public AmzaRegionChangeReceiver(AmzaStats amzaStats,
        RegionProvider regionProvider,
        WALs receivedUpdatesWAL,
        long applyReplicasIntervalInMillis) {
        this.amzaStats = amzaStats;
        this.regionProvider = regionProvider;
        this.receivedWAL = receivedUpdatesWAL;
        this.applyReplicasIntervalInMillis = applyReplicasIntervalInMillis;
    }

    public void receiveChanges(RegionName regionName, WALScanable changes) throws Exception {
        RowsChanged changed = receivedWAL.get(regionName).update(changes);
        amzaStats.received(changed);
        synchronized (this) {
            this.notifyAll();
        }
    }

    synchronized public void start() {
        if (scheduledThreadPool == null) {
            final int silenceBackToBackErrors = 100;
            scheduledThreadPool = Executors.newScheduledThreadPool(2);
            scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
                int failedToReceive = 0;

                @Override
                public void run() {
                    try {
                        failedToReceive = 0;
                        applyReceivedChanges();
                    } catch (Exception x) {
                        LOG.debug("Failing to replay apply replication.", x);
                        if (failedToReceive % silenceBackToBackErrors == 0) {
                            failedToReceive++;
                            LOG.error("Failing to replay apply replication.", x);
                        }
                    }
                }
            }, applyReplicasIntervalInMillis, applyReplicasIntervalInMillis, TimeUnit.MILLISECONDS);

            scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {
                    try {
                        compactReceivedChanges();
                    } catch (Exception x) {
                        LOG.error("Failing to compact.", x);
                    }
                }
            }, 1, 1, TimeUnit.MINUTES);
        }
    }

    synchronized public void stop() {
        if (scheduledThreadPool != null) {
            this.scheduledThreadPool.shutdownNow();
            this.scheduledThreadPool = null;
        }
    }

    private void applyReceivedChanges() throws Exception {
        while (true) {
            boolean appliedChanges = false;
            for (Map.Entry<RegionName, WALStorage> regionUpdates : receivedWAL.getAll()) {
                RegionName regionName = regionUpdates.getKey();
                WALStorage received = regionUpdates.getValue();
                RegionStore region = regionProvider.get(regionName);
                Long highWatermark = highwaterMarks.get(regionName);
                HighwaterInterceptor highwaterInterceptor = new HighwaterInterceptor("Apply", highWatermark, received);
                RowsChanged changed = region.commit(highwaterInterceptor);
                amzaStats.applied(changed);
                highwaterMarks.put(regionName, highwaterInterceptor.getHighwater());
                appliedChanges |= !changed.isEmpty();
            }
            if (!appliedChanges) {
                synchronized (this) {
                    this.wait();
                }
            }
        }
    }

    private void compactReceivedChanges() throws Exception {
        for (Map.Entry<RegionName, WALStorage> changes : receivedWAL.getAll()) {
            RegionName regionName = changes.getKey();
            WALStorage regionWAL = changes.getValue();
            Long highWatermark = highwaterMarks.get(regionName);
            if (highWatermark != null) {
                amzaStats.beginCompaction("Compacting Received:" + regionName);
                try {
                    regionWAL.compactTombstone(highWatermark); // TODO should this be plus 1
                } finally {
                    amzaStats.endCompaction("Compacting Received:" + regionName);
                }
            }
        }
    }
}
