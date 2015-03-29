package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaHostRing;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.MemoryWALIndex;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageUpateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author jonathan.colt
 */
public class AmzaRegionChangeTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private ScheduledExecutorService takerThreadPool;
    private final AmzaHostRing hostRingProvider;
    private final AmzaStats amzaStats;
    private final RegionProvider regionProvider;
    private final UpdatesTaker updatesTaker;
    private final HighwaterMarks highwaterMarks;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final long takeFromNeighborsIntervalInMillis;
    private final int numberOfTakerThreads;

    public AmzaRegionChangeTaker(AmzaStats amzaStats,
        AmzaHostRing amzaRing,
        RegionProvider regionProvider,
        HighwaterMarks highwaterMarks,
        UpdatesTaker updatesTaker,
        Optional<TakeFailureListener> takeFailureListener,
        long takeFromNeighborsIntervalInMillis,
        int numberOfTakerThreads) {

        this.amzaStats = amzaStats;
        this.hostRingProvider = amzaRing;
        this.regionProvider = regionProvider;
        this.highwaterMarks = highwaterMarks;
        this.updatesTaker = updatesTaker;
        this.takeFailureListener = takeFailureListener;
        this.takeFromNeighborsIntervalInMillis = takeFromNeighborsIntervalInMillis;
        this.numberOfTakerThreads = numberOfTakerThreads;
    }

    synchronized public void start() throws Exception {

        if (takerThreadPool == null) {
            takerThreadPool = Executors.newScheduledThreadPool(numberOfTakerThreads, new ThreadFactoryBuilder().setNameFormat("takerChanges-%d").build());
            for (int i = 0; i < numberOfTakerThreads; i++) {
                final int stripe = i;
                takerThreadPool.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            takeChanges(stripe);
                        } catch (Throwable x) {
                            LOG.warn("Shouldn't have gotten here. Implements please catch your expections.", x);
                        }
                    }
                }, takeFromNeighborsIntervalInMillis, takeFromNeighborsIntervalInMillis, TimeUnit.MILLISECONDS);
            }

        }
    }

    synchronized public void stop() throws Exception {
        if (takerThreadPool != null) {
            this.takerThreadPool.shutdownNow();
            this.takerThreadPool = null;
        }
    }

    public void takeChanges(int stripe) throws Exception {
        while (true) {
            boolean took = false;
            for (Map.Entry<RegionName, RegionStore> region : regionProvider.getAll()) {
                RegionName regionName = region.getKey();
                if (Math.abs(regionName.hashCode()) % numberOfTakerThreads == stripe) {
                    HostRing hostRing = hostRingProvider.getHostRing(regionName.getRingName());
                    RegionProperties regionProperties = regionProvider.getRegionProperties(regionName);
                    if (regionProperties != null && regionProperties.takeFromFactor > 0) {
                        took |= takeChanges(hostRing.getAboveRing(), regionName, regionProperties.takeFromFactor);
                    }
                }
            }
            if (!took) {
                break;
            }
        }
    }

    private boolean takeChanges(RingHost[] ringHosts, RegionName regionName, int takeFromFactor) throws Exception {
        final MutableInt taken = new MutableInt(0);
        int i = 0;
        final MutableInt leaps = new MutableInt(0);
        RegionStore regionStore = regionProvider.getRegionStore(regionName);
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
                    if (highwaterMark == null) {
                        highwaterMark = -1L;
                    }
                    TakeRowStream takeRowStream = new TakeRowStream(amzaStats, regionName, regionStore, ringHost, highwaterMark);
                    updatesTaker.streamingTakeUpdates(ringHost, regionName, highwaterMark, takeRowStream);
                    int updates = takeRowStream.flush();
                    if (updates > 0) {
                        highwaterMarks.set(ringHost, regionName, updates, takeRowStream.highWaterMark.longValue());
                        flushedChanges = true;
                    }
                    amzaStats.took(ringHost);
                    amzaStats.takeErrors.setCount(ringHost, 0);
                    if (takeFailureListener.isPresent()) {
                        takeFailureListener.get().tookFrom(ringHost);
                    }
                    taken.increment();
                    if (taken.intValue() >= takeFromFactor) {
                        return flushedChanges;
                    }
                    leaps.increment();
                    break;

                } catch (Exception x) {
                    if (takeFailureListener.isPresent()) {
                        takeFailureListener.get().failedToTake(ringHost, x);
                    }
                    if (amzaStats.takeErrors.count(ringHost) == 0) {
                        LOG.warn("Can't take from host:{}", ringHost);
                        LOG.trace("Can't take from host:{} region:{} takeFromFactor:{}", new Object[]{ringHost, regionName, takeFromFactor}, x);
                    }
                    amzaStats.takeErrors.add(ringHost);
                }
            }
        }
        return flushedChanges;
    }

    static class TakeRowStream implements RowStream {

        private final AmzaStats amzaStats;
        private final RegionName regionName;
        private final RegionStore regionStore;
        private final RingHost ringHost;
        private final MutableLong highWaterMark;
        private final TreeMap<WALKey, WALValue> batch = new TreeMap<>();
        private final MutableLong oldestTxId = new MutableLong(Long.MAX_VALUE);
        private final MutableLong lastTxId;
        private final AtomicInteger flushed = new AtomicInteger(0);
        private final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();

        public TakeRowStream(AmzaStats amzaStats,
            RegionName regionName,
            RegionStore regionStore,
            RingHost ringHost,
            long lastHighwaterMark) {
            this.amzaStats = amzaStats;
            this.regionName = regionName;
            this.regionStore = regionStore;
            this.ringHost = ringHost;
            this.highWaterMark = new MutableLong(lastHighwaterMark);
            this.lastTxId = new MutableLong(Long.MIN_VALUE);
        }

        @Override
        public boolean row(long rowFP, long txId, byte rowType, byte[] row) throws Exception {
            RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
            flushed.incrementAndGet();
            if (highWaterMark.longValue() < txId) {
                highWaterMark.setValue(txId);
            }
            if (oldestTxId.longValue() > txId) {
                oldestTxId.setValue(txId);
            }
            WALValue got = batch.get(walr.getKey());
            if (got == null) {
                batch.put(walr.getKey(), walr.getValue());
            } else {
                if (got.getTimestampId() < walr.getValue().getTimestampId()) {
                    batch.put(walr.getKey(), walr.getValue());
                }
            }
            if (lastTxId.longValue() == Long.MIN_VALUE) {
                lastTxId.setValue(txId);
            } else if (lastTxId.longValue() != txId) {
                lastTxId.setValue(txId);
                flush();
                batch.clear();
                oldestTxId.setValue(Long.MAX_VALUE);
            }
            return true;
        }

        public int flush() throws Exception {
            if (!batch.isEmpty()) {
                amzaStats.took(ringHost, regionName, batch.size(), oldestTxId.longValue());
                RowsChanged changes = regionStore.commit(WALStorageUpateMode.updateThenReplicate, new MemoryWALIndex(batch));
                amzaStats.tookApplied(ringHost, regionName, changes.getApply().size(), changes.getOldestRowTxId());
            }
            if (flushed.get() > 0) {
                amzaStats.took(ringHost, regionName, 0, Long.MAX_VALUE); // We are as up to date taking as is possible.
                amzaStats.tookApplied(ringHost, regionName, 0, Long.MAX_VALUE); // We are as up to date taking as is possible.
            }
            return flushed.get();
        }
    }

}
