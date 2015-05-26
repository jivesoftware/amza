package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.RingNeighbors;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowType;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALHighwater;
import com.jivesoftware.os.amza.shared.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.WALRow;
import com.jivesoftware.os.amza.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryPrimaryRowMarshaller;
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
import java.util.concurrent.atomic.AtomicReference;
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
    private final HighwaterStorage highwaterMarks;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final long takeFromNeighborsIntervalInMillis;
    private final int numberOfTakerThreads;
    private final boolean hardFlush;

    public AmzaRegionChangeTaker(AmzaStats amzaStats,
        AmzaRingReader amzaRing,
        RegionIndex regionIndex,
        RegionStripeProvider regionStripeProvider,
        RegionStripe[] stripes,
        HighwaterStorage highwaterMarks,
        UpdatesTaker updatesTaker,
        Optional<TakeFailureListener> takeFailureListener,
        long takeFromNeighborsIntervalInMillis,
        int numberOfTakerThreads,
        boolean hardFlush) {

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
        this.hardFlush = hardFlush;
    }

    synchronized public void start() throws Exception {

        if (masterTakerThreadPool == null) {
            slaveTakerThreadPool = Executors.newFixedThreadPool(numberOfTakerThreads);
            masterTakerThreadPool = Executors.newScheduledThreadPool(stripes.length + 1, new ThreadFactoryBuilder().setNameFormat(
                "masterTakeChanges-%d").build());
            for (int i = 0; i < stripes.length; i++) {
                final int stripe = i;
                masterTakerThreadPool.scheduleWithFixedDelay(() -> {
                    try {
                        takeChanges(stripes[stripe]);
                    } catch (Throwable x) {
                        LOG.warn("Shouldn't have gotten here. Implements please catch your expections.", x);
                    }
                }, takeFromNeighborsIntervalInMillis, takeFromNeighborsIntervalInMillis, TimeUnit.MILLISECONDS);
            }

            masterTakerThreadPool.scheduleWithFixedDelay(() -> {
                try {
                    takeChanges(regionStripeProvider.getSystemRegionStripe());
                } catch (Throwable x) {
                    LOG.warn("Shouldn't have gotten here. Implements please catch your expections.", x);
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
            final ListMultimap<RingMember, RegionName> tookFrom = Multimaps.synchronizedListMultimap(ArrayListMultimap.<RingMember, RegionName>create());
            List<Future<?>> futures = new ArrayList<>();
            for (final RegionName regionName : stripe.getActiveRegions()) {
                final RingNeighbors hostRing = amzaRing.getRingNeighbors(regionName.getRingName());
                final RegionProperties regionProperties = regionIndex.getProperties(regionName);
                if (regionProperties != null && regionProperties.takeFromFactor > 0) {

                    futures.add(slaveTakerThreadPool.submit(() -> {
                        try {
                            for (RingMember tookFromMember : takeChanges(hostRing.getAboveRing(), regionName, regionProperties.takeFromFactor)) {
                                tookFrom.put(tookFromMember, regionName);
                            }
                        } catch (Exception x) {
                            LOG.warn("Failed to take from " + regionName, x);
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
            stripe.flush(hardFlush);
            highwaterMarks.flush(tookFrom);
        }
    }

    private Set<RingMember> takeChanges(Entry<RingMember, RingHost>[] ring, RegionName regionName, int takeFromFactor) throws Exception {
        final MutableInt taken = new MutableInt(0);
        int i = 0;
        final MutableInt leaps = new MutableInt(0);
        Optional<RegionStripe> regionStripe = regionStripeProvider.getRegionStripe(regionName);
        Set<RingMember> tookFrom = new HashSet<>();
        if (regionStripe.isPresent()) {
            DONE:
            while (i < ring.length) {
                i = (leaps.intValue() * 2);
                for (; i < ring.length; i++) {
                    Entry<RingMember, RingHost> node = ring[i];
                    if (node == null) {
                        continue;
                    }
                    ring[i] = null;
                    RingMember ringMember = node.getKey();
                    try {
                        Long highwaterMark = highwaterMarks.get(ringMember, regionName);
                        if (highwaterMark == null) {
                            highwaterMark = -1L;
                        }
                        TakeRowStream takeRowStream = new TakeRowStream(amzaStats, regionName, regionStripe.get(), ringMember, highwaterMark);
                        int updates = 0;
                        try {
                            Map<RingMember, Long> otherHighwaterMarks = updatesTaker.streamingTakeUpdates(node, regionName, highwaterMark, takeRowStream);
                            updates = takeRowStream.flush();
                            if (updates > 0) {
                                tookFrom.add(ringMember);
                            }
                            for (Entry<RingMember, Long> otherHighwaterMark : otherHighwaterMarks.entrySet()) {
                                takeRowStream.flushedHighwatermarks.merge(otherHighwaterMark.getKey(), otherHighwaterMark.getValue(), (a, b) -> {
                                    return Math.max(a, b);
                                });
                            }
                        } catch (Exception x) {
                            LOG.warn("Failure while streaming takes.", x);
                        }

                        for (Entry<RingMember, Long> entry : takeRowStream.flushedHighwatermarks.entrySet()) {
                            highwaterMarks.setIfLarger(entry.getKey(), regionName, updates, entry.getValue());
                        }

                        amzaStats.took(ringMember);
                        amzaStats.takeErrors.setCount(ringMember, 0);
                        if (takeFailureListener.isPresent()) {
                            takeFailureListener.get().tookFrom(node);
                        }
                        taken.increment();
                        if (taken.intValue() >= takeFromFactor) {
                            break DONE;
                        }
                        leaps.increment();
                        break;

                    } catch (Exception x) {
                        if (takeFailureListener.isPresent()) {
                            takeFailureListener.get().failedToTake(node, x);
                        }
                        if (amzaStats.takeErrors.count(node) == 0) {
                            LOG.warn("Can't take from host:{}", node);
                            LOG.trace("Can't take from host:{} region:{} takeFromFactor:{}", new Object[]{node, regionName, takeFromFactor}, x);
                        }
                        amzaStats.takeErrors.add(ringMember);
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
        private final RingMember ringMember;
        private final MutableLong highWaterMark;
        private final Map<WALKey, WALValue> batch = new HashMap<>();
        private final MutableLong oldestTxId = new MutableLong(Long.MAX_VALUE);
        private final MutableLong lastTxId;
        private final AtomicInteger flushed = new AtomicInteger(0);
        private final AtomicReference<WALHighwater> highwater = new AtomicReference<>();
        private final BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller(); // TODO ah pass this in??
        private final BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller = new BinaryHighwaterRowMarshaller(); // TODO ah pass this in??
        private final Map<RingMember, Long> flushedHighwatermarks = new HashMap<>();

        public TakeRowStream(AmzaStats amzaStats,
            RegionName regionName,
            RegionStripe regionStripe,
            RingMember ringMember,
            long lastHighwaterMark) {
            this.amzaStats = amzaStats;
            this.regionName = regionName;
            this.regionStripe = regionStripe;
            this.ringMember = ringMember;
            this.highWaterMark = new MutableLong(lastHighwaterMark);
            this.lastTxId = new MutableLong(Long.MIN_VALUE);
        }

        @Override
        public boolean row(long rowFP, long txId, RowType rowType, byte[] row) throws Exception {
            if (rowType == RowType.primary) {
                if (lastTxId.longValue() == Long.MIN_VALUE) {
                    lastTxId.setValue(txId);
                } else if (lastTxId.longValue() != txId) {
                    lastTxId.setValue(txId);
                    flush();
                    batch.clear();
                    oldestTxId.setValue(Long.MAX_VALUE);
                }

                WALRow walr = primaryRowMarshaller.fromRow(row);
                flushed.incrementAndGet();
                if (highWaterMark.longValue() < txId) {
                    highWaterMark.setValue(txId);
                }
                if (oldestTxId.longValue() > txId) {
                    oldestTxId.setValue(txId);
                }
                WALValue got = batch.get(walr.key);
                if (got == null) {
                    batch.put(walr.key, walr.value);
                } else {
                    if (got.getTimestampId() < walr.value.getTimestampId()) {
                        batch.put(walr.key, walr.value);
                    }
                }

            } else if (rowType == RowType.highwater) {
                highwater.set(binaryHighwaterRowMarshaller.fromBytes(row));
            }
            return true;
        }

        public int flush() throws Exception {
            if (!batch.isEmpty()) {
                amzaStats.took(ringMember, regionName, batch.size(), oldestTxId.longValue());
                WALHighwater walh = highwater.get();
                MemoryWALUpdates updates = new MemoryWALUpdates(batch, walh);
                RowsChanged changes = regionStripe.commit(regionName, null, WALStorageUpdateMode.noReplication, updates);
                amzaStats.tookApplied(ringMember, regionName, changes.getApply().size(), changes.getOldestRowTxId());
                if (walh != null) {
                    for (RingMemberHighwater memberHighwater : walh.ringMemberHighwater) {
                        flushedHighwatermarks.merge(memberHighwater.ringMember, memberHighwater.transactionId, (a, b) -> {
                            return Math.max(a, b);
                        });
                    }
                }
                flushedHighwatermarks.merge(ringMember, highWaterMark.longValue(), (a, b) -> {
                    return Math.max(a, b);
                });
            }
            highwater.set(null);
            if (flushed.get() > 0) {
                amzaStats.took(ringMember, regionName, 0, Long.MAX_VALUE); // We are as up to date taking as is possible.
                amzaStats.tookApplied(ringMember, regionName, 0, Long.MAX_VALUE); // We are as up to date taking as is possible.
            }
            return flushed.get();
        }
    }

}
