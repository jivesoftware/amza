package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.RingNeighbors;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowType;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.TxRegionStatus;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
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
import java.util.Collections;
import java.util.HashMap;
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
public class RegionChangeTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private ScheduledExecutorService masterTakerThreadPool;
    private ExecutorService slaveTakerThreadPool;
    private final AmzaRingReader amzaRingReader;
    private final AmzaStats amzaStats;
    private final RegionIndex regionIndex;
    private final RegionStripeProvider regionStripeProvider;
    private final RegionStripe[] stripes;
    private final UpdatesTaker updatesTaker;
    private final HighwaterStorage highwaterStorage;
    private final RegionStatusStorage regionStatusStorage;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final long takeFromNeighborsIntervalInMillis;
    private final int numberOfTakerThreads;
    private final boolean hardFlush;

    public RegionChangeTaker(AmzaStats amzaStats,
        AmzaRingReader amzaRingReader,
        RegionIndex regionIndex,
        RegionStripeProvider regionStripeProvider,
        RegionStripe[] stripes,
        HighwaterStorage highwaterStorage,
        RegionStatusStorage regionStatusStorage,
        UpdatesTaker updatesTaker,
        Optional<TakeFailureListener> takeFailureListener,
        long takeFromNeighborsIntervalInMillis,
        int numberOfTakerThreads,
        boolean hardFlush) {

        this.amzaStats = amzaStats;
        this.amzaRingReader = amzaRingReader;
        this.regionIndex = regionIndex;
        this.regionStripeProvider = regionStripeProvider;
        this.stripes = stripes;
        this.highwaterStorage = highwaterStorage;
        this.regionStatusStorage = regionStatusStorage;
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
            ListMultimap<RingMember, VersionedRegionName> flushMap = Multimaps.synchronizedListMultimap(
                ArrayListMultimap.<RingMember, VersionedRegionName>create());
            ListMultimap<RingMember, VersionedRegionName> onlineMap = Multimaps.synchronizedListMultimap(
                ArrayListMultimap.<RingMember, VersionedRegionName>create());
            Set<VersionedRegionName> ketchupSet = Collections.newSetFromMap(Maps.newConcurrentMap());

            List<Future<?>> futures = new ArrayList<>();
            stripe.txAllRegions((versionedRegionName, regionStatus) -> {
                if (regionStatus == TxRegionStatus.Status.KETCHUP || regionStatus == TxRegionStatus.Status.ONLINE) {
                    final RingNeighbors hostRing = amzaRingReader.getRingNeighbors(versionedRegionName.getRegionName().getRingName());
                    final RegionProperties regionProperties = regionIndex.getProperties(versionedRegionName.getRegionName());
                    if (regionProperties != null && regionProperties.takeFromFactor > 0) {

                        futures.add(slaveTakerThreadPool.submit(() -> {
                            try {
                                List<TookResult> took = takeChanges(hostRing.getAboveRing(), stripe, versionedRegionName, regionProperties.takeFromFactor);
                                boolean allInKetchup = true;
                                for (TookResult t : took) {
                                    if (t.tookFully || t.tookError) {
                                        allInKetchup = false;
                                    }
                                    if (t.tookFully) {
                                        onlineMap.put(t.versionedRingMember, versionedRegionName);
                                    }
                                    if (t.tookAny) {
                                        flushMap.put(t.versionedRingMember, versionedRegionName);
                                    }
                                }
                                if (allInKetchup) {
                                    ketchupSet.add(versionedRegionName);
                                }
                            } catch (Exception x) {
                                LOG.warn("Failed to take from " + versionedRegionName, x);
                            }
                        }));
                    }
                }
                return null;
            });

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException x) {
                    LOG.warn("Failed to take.", x);
                }
            }

            regionStatusStorage.markAsOnline(onlineMap);
            if (!ketchupSet.isEmpty()) {
                for (VersionedRegionName versionedRegionName : ketchupSet) {
                    regionStatusStorage.elect(amzaRingReader.getRing(versionedRegionName.getRegionName().getRingName()).keySet(), versionedRegionName);
                }
            }
            if (flushMap.isEmpty()) {
                break;
            } else {
                stripe.flush(hardFlush);
                highwaterStorage.flush(flushMap);
            }
        }
    }

    static class TookResult {

        public final RingMember versionedRingMember;
        public final boolean tookAny;
        public final boolean tookFully;
        public final boolean tookError;

        public TookResult(RingMember ringMember, boolean tookAny, boolean fullyTook, boolean tookError) {
            this.versionedRingMember = ringMember;
            this.tookAny = tookAny;
            this.tookFully = fullyTook;
            this.tookError = tookError;
        }

    }

    private List<TookResult> takeChanges(Entry<RingMember, RingHost>[] ring,
        RegionStripe regionStripe,
        VersionedRegionName versionedRegionName,
        int takeFromFactor) throws Exception {

        final MutableInt taken = new MutableInt(0);
        int i = 0;
        final MutableInt leaps = new MutableInt(0);
        List<TookResult> tookFrom = new ArrayList<>();
        DONE:
        while (i < ring.length) {
            i = (leaps.intValue() * 2);
            for (; i < ring.length; i++) {
                if (ring[i] == null) {
                    continue;
                }
                Entry<RingMember, RingHost> node = ring[i];
                ring[i] = null;

                RingMember ringMember = node.getKey();
                Long highwaterMark = highwaterStorage.get(ringMember, versionedRegionName);
                if (highwaterMark == null) {
                    highwaterMark = -1L;
                }
                TakeRowStream takeRowStream = new TakeRowStream(amzaStats,
                    versionedRegionName,
                    regionStripe,
                    ringMember,
                    highwaterMark);

                boolean tookAny = false;
                boolean fullyTook = false;
                boolean tookError = false;
                int updates = 0;
                try {
                    Map<RingMember, Long> otherHighwaterMarks = updatesTaker.streamingTakeUpdates(node,
                        versionedRegionName.getRegionName(),
                        highwaterMark,
                        takeRowStream);

                    updates = takeRowStream.flush();
                    tookAny = updates > 0;
                    if (otherHighwaterMarks != null) {
                        for (Entry<RingMember, Long> otherHighwaterMark : otherHighwaterMarks.entrySet()) {
                            takeRowStream.flushedHighwatermarks.merge(otherHighwaterMark.getKey(), otherHighwaterMark.getValue(), (a, b) -> {
                                return Math.max(a, b);
                            });
                        }
                        fullyTook = true;
                    }
                } catch (Exception x) {
                    tookError = true;
                    if (takeFailureListener.isPresent()) {
                        takeFailureListener.get().failedToTake(node, x);
                    }
                    if (amzaStats.takeErrors.count(node) == 0) {
                        LOG.warn("Can't take from host:{}", node);
                        LOG.trace("Can't take from host:{} region:{} takeFromFactor:{}", new Object[]{node, versionedRegionName, takeFromFactor}, x);
                    }
                    amzaStats.takeErrors.add(ringMember);
                }

                if (takeRowStream.haveFlushed()) {
                    tookFrom.add(new TookResult(ringMember, tookAny, fullyTook, tookError));
                }

                for (Entry<RingMember, Long> entry : takeRowStream.flushedHighwatermarks.entrySet()) {
                    highwaterStorage.setIfLarger(entry.getKey(), versionedRegionName, updates, entry.getValue());
                }

                if (fullyTook) {
                    amzaStats.took(ringMember);
                    amzaStats.takeErrors.setCount(ringMember, 0);
                    if (takeFailureListener.isPresent()) {
                        takeFailureListener.get().tookFrom(node);
                    }
                    taken.increment();
                    if (taken.intValue() >= takeFromFactor) {
                        break DONE;
                    }
                }
                leaps.increment();
                break;
            }
        }
        return tookFrom;

    }

    static class TakeRowStream implements RowStream {

        private final AmzaStats amzaStats;
        private final VersionedRegionName versionedRegionName;
        private final RegionStripe regionStripe;
        private final RingMember ringMember;
        private final MutableLong highWaterMark;
        private final Map<WALKey, WALValue> batch = new HashMap<>();
        private final MutableLong oldestTxId = new MutableLong(Long.MAX_VALUE);
        private final MutableLong lastTxId;
        private final AtomicInteger streamed = new AtomicInteger(0);
        private final AtomicInteger flushed = new AtomicInteger(0);
        private final AtomicReference<WALHighwater> highwater = new AtomicReference<>();
        private final BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller(); // TODO ah pass this in??
        private final BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller = new BinaryHighwaterRowMarshaller(); // TODO ah pass this in??
        private final Map<RingMember, Long> flushedHighwatermarks = new HashMap<>();

        public TakeRowStream(AmzaStats amzaStats,
            VersionedRegionName versionedRegionName,
            RegionStripe regionStripe,
            RingMember ringMember,
            long lastHighwaterMark) {
            this.amzaStats = amzaStats;
            this.versionedRegionName = versionedRegionName;
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
                streamed.incrementAndGet();
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

        public boolean haveFlushed() {
            return flushed.get() > 0;
        }

        public int flush() throws Exception {
            if (!batch.isEmpty()) {
                amzaStats.took(ringMember, versionedRegionName.getRegionName(), batch.size(), oldestTxId.longValue());
                WALHighwater walh = highwater.get();
                MemoryWALUpdates updates = new MemoryWALUpdates(batch, walh);
                RowsChanged changes = regionStripe.commit(versionedRegionName.getRegionName(),
                    Optional.of(versionedRegionName.getRegionVersion()),
                    null,
                    WALStorageUpdateMode.noReplication, updates);
                if (changes != null) {
                    amzaStats.tookApplied(ringMember, versionedRegionName.getRegionName(), changes.getApply().size(), changes.getOldestRowTxId());
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
                    flushed.set(streamed.get());
                }
            }
            highwater.set(null);
            if (streamed.get() > 0) {
                amzaStats.took(ringMember, versionedRegionName.getRegionName(), 0, Long.MAX_VALUE); // We are as up to date taking as is possible.
                amzaStats.tookApplied(ringMember, versionedRegionName.getRegionName(), 0, Long.MAX_VALUE); // We are as up to date taking as is possible.
            }
            return streamed.get();
        }
    }

}
