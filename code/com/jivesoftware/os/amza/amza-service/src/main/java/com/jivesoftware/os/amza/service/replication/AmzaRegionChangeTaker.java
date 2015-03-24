package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaHostRing;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.MemoryWALIndex;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author jonathan.colt
 */
public class AmzaRegionChangeTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private ScheduledExecutorService scheduledThreadPool;
    private final AmzaHostRing hostRingProvider;
    private final AmzaStats amzaStats;
    private final RegionProvider regionProvider;
    private final int takeFromFactor;
    private final UpdatesTaker updatesTaker;
    private final HighwaterMarks highwaterMarks;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final long takeFromNeighborsIntervalInMillis;

    public AmzaRegionChangeTaker(AmzaStats amzaStats,
        AmzaHostRing amzaRing,
        RegionProvider regionProvider,
        int takeFromFactor,
        HighwaterMarks highwaterMarks,
        UpdatesTaker updatesTaker,
        Optional<TakeFailureListener> takeFailureListener,
        long takeFromNeighborsIntervalInMillis) {

        this.amzaStats = amzaStats;
        this.hostRingProvider = amzaRing;
        this.regionProvider = regionProvider;
        this.takeFromFactor = takeFromFactor;
        this.highwaterMarks = highwaterMarks;
        this.updatesTaker = updatesTaker;
        this.takeFailureListener = takeFailureListener;
        this.takeFromNeighborsIntervalInMillis = takeFromNeighborsIntervalInMillis;
    }

    synchronized public void start() throws Exception {

        final int silenceBackToBackErrors = 100;
        if (scheduledThreadPool == null) {
            scheduledThreadPool = Executors.newScheduledThreadPool(4);

            scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
                int failedToTake = 0;

                @Override
                public void run() {
                    try {
                        failedToTake = 0;
                        takeChanges();
                    } catch (Exception x) {
                        LOG.debug("Failing to take from above and below.", x);
                        if (failedToTake % silenceBackToBackErrors == 0) {
                            failedToTake++;
                            LOG.error("Failing to take from above and below.");
                        }
                    }
                }
            }, takeFromNeighborsIntervalInMillis, takeFromNeighborsIntervalInMillis, TimeUnit.MILLISECONDS);

        }
    }

    synchronized public void stop() throws Exception {
        if (scheduledThreadPool != null) {
            this.scheduledThreadPool.shutdownNow();
            this.scheduledThreadPool = null;
        }
    }

    public void takeChanges() throws Exception {
        while (true) {
            boolean took = false;
            for (Map.Entry<RegionName, RegionStore> region : regionProvider.getAll()) {
                RegionName regionName = region.getKey();
                HostRing hostRing = hostRingProvider.getHostRing(regionName.getRingName());
                took |= takeChanges(hostRing.getAboveRing(), regionName);
            }
            if (!took) {
                break;
            }
        }
    }

    private boolean takeChanges(RingHost[] ringHosts, RegionName regionName) throws Exception {
        final MutableInt taken = new MutableInt(0);
        int i = 0;
        final MutableInt leaps = new MutableInt(0);
        RegionStore regionStore = regionProvider.get(regionName);
        int failedToTake = 0;
        boolean flushedChanges = false;
        while (i < ringHosts.length) {
            i = (leaps.intValue() * 2);
            for (; i < ringHosts.length; i++) {
                RingHost ringHost = ringHosts[i];
                if (ringHost == null) {
                    continue;
                }
                ringHosts[i] = null;
                try {
                    Long highwaterMark = highwaterMarks.get(ringHost, regionName);
                    TakeRowStream takeRowStream = new TakeRowStream(amzaStats, regionStore, ringHost, regionName, highwaterMarks);
                    updatesTaker.takeUpdates(ringHost, regionName, highwaterMark, takeRowStream);
                    flushedChanges |= takeRowStream.flush();
                    amzaStats.took(ringHost);
                    if (takeFailureListener.isPresent()) {
                        takeFailureListener.get().tookFrom(ringHost);
                    }
                    taken.increment();
                    failedToTake++;
                    if (taken.intValue() >= takeFromFactor) {
                        return flushedChanges;
                    }
                    leaps.increment();
                    break;

                } catch (Exception x) {
                    if (takeFailureListener.isPresent()) {
                        takeFailureListener.get().failedToTake(ringHost, x);
                    }
                    LOG.debug("Can't takeFrom host:" + ringHost, x);
                    if (failedToTake == 0) {
                        failedToTake++;
                        LOG.warn("Can't takeFrom host:" + ringHost + " " + x.toString());
                    }
                }
            }
        }
        return flushedChanges;
    }

    // TODO fix known issues around how highwater marks are handled
    static class TakeRowStream implements WALScan {

        private final AmzaStats amzaStats;
        private final RegionStore regionStore;
        private final RingHost ringHost;
        private final RegionName regionName;
        private final HighwaterMarks highWaterMarks;
        private final MutableLong highWaterMark;
        private final TreeMap<WALKey, WALValue> batch = new TreeMap<>();

        public TakeRowStream(AmzaStats amzaStats, RegionStore regionStore,
            RingHost ringHost,
            RegionName regionName,
            HighwaterMarks highWaterMarks) {
            this.amzaStats = amzaStats;
            this.regionStore = regionStore;
            this.ringHost = ringHost;
            this.regionName = regionName;
            this.highWaterMarks = highWaterMarks;
            this.highWaterMark = new MutableLong(highWaterMarks.get(ringHost, regionName));
        }

        @Override
        public boolean row(long txId, WALKey key, WALValue value) throws Exception {
            if (highWaterMark.longValue() < txId) {
                highWaterMark.setValue(txId);
            }
            WALValue got = batch.get(key);
            if (got == null) {
                batch.put(key, value);
            } else {
                if (got.getTimestampId() < value.getTimestampId()) {
                    batch.put(key, value);
                }
            }
            return true;
        }

        public boolean flush() throws Exception {
            boolean flushed = false;
            if (!batch.isEmpty()) {
                RowsChanged changes = regionStore.commit(new MemoryWALIndex(batch));
                amzaStats.took(ringHost, changes);
                flushed |= !changes.isEmpty();
                highWaterMarks.set(ringHost, regionName, highWaterMark.longValue());
            }
            if (highWaterMark.longValue() < 0) {
                highWaterMarks.set(ringHost, regionName, 0);
            }
            return flushed;
        }
    }

}
