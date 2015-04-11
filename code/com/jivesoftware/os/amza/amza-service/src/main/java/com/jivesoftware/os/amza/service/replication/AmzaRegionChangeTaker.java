package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.HostRing;
import com.jivesoftware.os.amza.shared.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    private ScheduledExecutorService masterTakerThreadPool;
    private ExecutorService slaveTakerThreadPool;
    private final AmzaRingReader amzaRing;
    private final AmzaStats amzaStats;
    private final RegionIndex regionIndex;
    private final RegionStripeProvider regionStripeProvider;
    private final RegionStripe[] stripes;
    private final UpdatesTaker updatesTaker;
    private final HighwaterMarks highwaterMarks;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final long takeFromNeighborsIntervalInMillis;
    private final int numberOfTakerThreads;

    public AmzaRegionChangeTaker(AmzaStats amzaStats,
        AmzaRingReader amzaRing,
        RegionIndex regionIndex,
        RegionStripeProvider regionStripeProvider,
        RegionStripe[] stripes,
        HighwaterMarks highwaterMarks,
        UpdatesTaker updatesTaker,
        Optional<TakeFailureListener> takeFailureListener,
        long takeFromNeighborsIntervalInMillis,
        int numberOfTakerThreads) {

        this.amzaStats = amzaStats;
        this.amzaRing = amzaRing;
        this.regionIndex = regionIndex;
        this.regionStripeProvider = regionStripeProvider;
        this.stripes = stripes;
        this.highwaterMarks = highwaterMarks;
        this.updatesTaker = updatesTaker;
        this.takeFailureListener = takeFailureListener;
        this.takeFromNeighborsIntervalInMillis = takeFromNeighborsIntervalInMillis;
        this.numberOfTakerThreads = numberOfTakerThreads;
    }

    synchronized public void start() throws Exception {

        if (masterTakerThreadPool == null) {
            slaveTakerThreadPool = Executors.newFixedThreadPool(numberOfTakerThreads);
            masterTakerThreadPool = Executors.newScheduledThreadPool(stripes.length + 1, new ThreadFactoryBuilder().setNameFormat(
                "masterTakeChanges-%d").build());
            for (int i = 0; i < stripes.length; i++) {
                final int stripe = i;
                masterTakerThreadPool.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            takeChanges(stripes[stripe]);
                        } catch (Throwable x) {
                            LOG.warn("Shouldn't have gotten here. Implements please catch your expections.", x);
                        }
                    }
                }, takeFromNeighborsIntervalInMillis, takeFromNeighborsIntervalInMillis, TimeUnit.MILLISECONDS);
            }

            masterTakerThreadPool.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {
                    try {
                        takeChanges(regionStripeProvider.getSystemRegionStripe());
                    } catch (Throwable x) {
                        LOG.warn("Shouldn't have gotten here. Implements please catch your expections.", x);
                    }
                }
            }, takeFromNeighborsIntervalInMillis, takeFromNeighborsIntervalInMillis, TimeUnit.MILLISECONDS);
        }
    }

    synchronized public void stop() throws Exception {
        if (masterTakerThreadPool != null) {
            this.slaveTakerThreadPool.shutdownNow();
            this.slaveTakerThreadPool = null;
            this.masterTakerThreadPool.shutdownNow();
            this.masterTakerThreadPool = null;
        }
    }

    public void takeChanges(RegionStripe stripe) throws Exception {
        while (true) {
            final ListMultimap<RingHost, RegionName> tookFrom = Multimaps.synchronizedListMultimap(ArrayListMultimap.<RingHost, RegionName>create());
            List<Future<?>> futures = new ArrayList<>();
            for (final RegionName regionName : stripe.getActiveRegions()) {
                final HostRing hostRing = amzaRing.getHostRing(regionName.getRingName());
                final RegionProperties regionProperties = regionIndex.getProperties(regionName);
                if (regionProperties != null && regionProperties.takeFromFactor > 0) {

                    futures.add(slaveTakerThreadPool.submit(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                for (RingHost tookFromHost : takeChanges(hostRing.getAboveRing(), regionName, regionProperties.takeFromFactor)) {
                                    tookFrom.put(tookFromHost, regionName);
                                }
                            } catch (Exception x) {
                                LOG.warn("Failed to take from " + regionName, x);
                            }
                        }
                    }));
                }
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException x) {
                    LOG.warn("Failed to take.", x);
                }
            }

            if (tookFrom.isEmpty()) {
                break;
            }
            stripe.flush(true);
            highwaterMarks.flush(tookFrom);
        }
    }

    private Set<RingHost> takeChanges(RingHost[] ringHosts, RegionName regionName, int takeFromFactor) throws Exception {
        final MutableInt taken = new MutableInt(0);
        int i = 0;
        final MutableInt leaps = new MutableInt(0);
        Optional<RegionStripe> regionStripe = regionStripeProvider.getRegionStripe(regionName);
        Set<RingHost> tookFrom = new HashSet<>();
        if (regionStripe.isPresent()) {
            DONE:
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
                        TakeRowStream takeRowStream = new TakeRowStream(amzaStats, regionName, regionStripe.get(), ringHost, highwaterMark);
                        Map<RingHost, Long> otherHighwaterMarks = updatesTaker.streamingTakeUpdates(ringHost, regionName, highwaterMark, takeRowStream);
                        int updates = takeRowStream.flush();
                        for (Entry<RingHost, Long> otherHighwaterMark : otherHighwaterMarks.entrySet()) {
                            highwaterMarks.setIfLarger(otherHighwaterMark.getKey(), regionName, updates, otherHighwaterMark.getValue());
                        }

                        if (updates > 0) {
                            highwaterMarks.setIfLarger(ringHost, regionName, updates, takeRowStream.highWaterMark.longValue());
                            tookFrom.add(ringHost);
                        }
                        amzaStats.took(ringHost);
                        amzaStats.takeErrors.setCount(ringHost, 0);
                        if (takeFailureListener.isPresent()) {
                            takeFailureListener.get().tookFrom(ringHost);
                        }
                        taken.increment();
                        if (taken.intValue() >= takeFromFactor) {
                            break DONE;
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
        } else {
            LOG.warn("Trying to take from a region:{} is not yet available.", regionName);
        }
        return tookFrom;

    }

    static class TakeRowStream implements RowStream {

        private final AmzaStats amzaStats;
        private final RegionName regionName;
        private final RegionStripe regionStripe;
        private final RingHost ringHost;
        private final MutableLong highWaterMark;
        private final Map<WALKey, WALValue> batch = new HashMap<>();
        private final MutableLong oldestTxId = new MutableLong(Long.MAX_VALUE);
        private final MutableLong lastTxId;
        private final AtomicInteger flushed = new AtomicInteger(0);
        private final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();

        public TakeRowStream(AmzaStats amzaStats,
            RegionName regionName,
            RegionStripe regionStripe,
            RingHost ringHost,
            long lastHighwaterMark) {
            this.amzaStats = amzaStats;
            this.regionName = regionName;
            this.regionStripe = regionStripe;
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
                RowsChanged changes = regionStripe.commit(regionName, null, WALStorageUpdateMode.noReplication, new MemoryWALUpdates(batch));
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
