package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.ring.RingNeighbors;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker.AckTaken;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker.StreamingTakeResult;
import com.jivesoftware.os.amza.shared.wal.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.storage.WALRow;
import com.jivesoftware.os.amza.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
 * @author jonathan.colt
 */
public class PartitionChangeTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private ScheduledExecutorService masterTakerThreadPool;
    private ExecutorService slaveTakerThreadPool;
    private ExecutorService ackerThreadPool;
    private final AmzaRingReader amzaRingReader;
    private final RingHost ringHost;
    private final AmzaStats amzaStats;
    private final PartitionIndex partitionIndex;
    private final PartitionStripeProvider partitionStripeProvider;
    private final PartitionStripe[] stripes;
    private final UpdatesTaker updatesTaker;
    private final HighwaterStorage highwaterStorage;
    private final PartitionStatusStorage partitionStatusStorage;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final long takeFromNeighborsIntervalInMillis;
    private final int numberOfTakerThreads;
    private final int numberOfAckerThreads;
    private final boolean hardFlush;

    public PartitionChangeTaker(AmzaStats amzaStats,
        AmzaRingReader amzaRingReader,
        RingHost ringHost,
        PartitionIndex partitionIndex,
        PartitionStripeProvider partitionStripeProvider,
        PartitionStripe[] stripes,
        HighwaterStorage highwaterStorage,
        PartitionStatusStorage partitionStatusStorage,
        UpdatesTaker updatesTaker,
        Optional<TakeFailureListener> takeFailureListener,
        long takeFromNeighborsIntervalInMillis,
        int numberOfTakerThreads,
        int numberOfAckerThreads,
        boolean hardFlush) {

        this.amzaStats = amzaStats;
        this.amzaRingReader = amzaRingReader;
        this.ringHost = ringHost;
        this.partitionIndex = partitionIndex;
        this.partitionStripeProvider = partitionStripeProvider;
        this.stripes = stripes;
        this.highwaterStorage = highwaterStorage;
        this.partitionStatusStorage = partitionStatusStorage;
        this.updatesTaker = updatesTaker;
        this.takeFailureListener = takeFailureListener;
        this.takeFromNeighborsIntervalInMillis = takeFromNeighborsIntervalInMillis;
        this.numberOfTakerThreads = numberOfTakerThreads;
        this.numberOfAckerThreads = numberOfAckerThreads;
        this.hardFlush = hardFlush;
    }

    synchronized public void start() throws Exception {

        if (masterTakerThreadPool == null) {
            slaveTakerThreadPool = Executors.newFixedThreadPool(numberOfTakerThreads, new ThreadFactoryBuilder().setNameFormat(
                "slaveTakeChanges-%d").build());
            ackerThreadPool = Executors.newFixedThreadPool(numberOfAckerThreads);
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
                    takeChanges(partitionStripeProvider.getSystemPartitionStripe());
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

    static class RingMemberAndHost {

        private final RingMember ringMember;
        private final RingHost ringHost;

        public RingMemberAndHost(RingMember ringMember, RingHost ringHost) {
            this.ringMember = ringMember;
            this.ringHost = ringHost;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 11 * hash + Objects.hashCode(this.ringMember);
            hash = 11 * hash + Objects.hashCode(this.ringHost);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final RingMemberAndHost other = (RingMemberAndHost) obj;
            if (!Objects.equals(this.ringMember, other.ringMember)) {
                return false;
            }
            if (!Objects.equals(this.ringHost, other.ringHost)) {
                return false;
            }
            return true;
        }

    }

    public void takeChanges(PartitionStripe stripe) throws Exception {
        while (true) {
            LOG.inc("takeAll>allStripes");
            LOG.inc("takeAll>" + stripe.getName());
            LOG.startTimer("takeAll>allStripes");
            LOG.startTimer("takeAll>" + stripe.getName());
            try {
                ListMultimap<RingMember, VersionedPartitionName> flushMap = Multimaps.synchronizedListMultimap(
                    ArrayListMultimap.<RingMember, VersionedPartitionName>create());
                Set<VersionedPartitionName> onlineSet = Collections.newSetFromMap(Maps.newConcurrentMap());
                Set<VersionedPartitionName> ketchupSet = Collections.newSetFromMap(Maps.newConcurrentMap());
                SetMultimap<VersionedPartitionName, RingMember> membersUnreachable = Multimaps.synchronizedSetMultimap(
                    HashMultimap.<VersionedPartitionName, RingMember>create());

                ListMultimap<RingMemberAndHost, AckTaken> ackMap = Multimaps.synchronizedListMultimap(ArrayListMultimap.<RingMemberAndHost, AckTaken>create());

                List<Future<?>> futures = new ArrayList<>();
                stripe.txAllPartitions((versionedPartitionName, partitionStatus) -> {
                    if (partitionStatus == TxPartitionStatus.Status.KETCHUP || partitionStatus == TxPartitionStatus.Status.ONLINE) {
                        final RingNeighbors hostRing = amzaRingReader.getRingNeighbors(versionedPartitionName.getPartitionName().getRingName());
                        final PartitionProperties partitionProperties = partitionIndex.getProperties(versionedPartitionName.getPartitionName());
                        if (partitionProperties != null) {
                            if (partitionProperties.takeFromFactor > 0) {
                                futures.add(slaveTakerThreadPool.submit(() -> {
                                    try {
                                        List<TookResult> took = takeChanges(hostRing.getAboveRing(), stripe, versionedPartitionName,
                                            partitionProperties.takeFromFactor);

                                        boolean allInKetchup = true;
                                        boolean oneTookFully = false;
                                        for (TookResult t : took) {
                                            if (t.tookFully || t.tookError) {
                                                allInKetchup = false;
                                            }
                                            if (t.tookFully) {
                                                oneTookFully = true;
                                                ackMap.put(new RingMemberAndHost(t.ringMember, t.ringHost), new AckTaken(t.versionedPartitionName, t.txId));
                                            }
                                            if (t.flushedAny) {
                                                flushMap.put(t.ringMember, versionedPartitionName);
                                            }
                                            if (t.tookUnreachable) {
                                                membersUnreachable.put(versionedPartitionName, t.ringMember);
                                            }
                                        }
                                        if (allInKetchup) {
                                            ketchupSet.add(versionedPartitionName);
                                        }
                                        if (oneTookFully) {
                                            onlineSet.add(versionedPartitionName);
                                        }
                                    } catch (Exception x) {
                                        LOG.warn("Failed to take from " + versionedPartitionName, x);
                                    }
                                }));
                            } else {
                                onlineSet.add(versionedPartitionName);
                            }
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

                for (VersionedPartitionName versionedPartitionName : onlineSet) {
                    partitionStatusStorage.markAsOnline(versionedPartitionName);
                }
                for (VersionedPartitionName versionedPartitionName : ketchupSet) {
                    partitionStatusStorage.elect(amzaRingReader.getRing(versionedPartitionName.getPartitionName().getRingName()).keySet(),
                        membersUnreachable.get(versionedPartitionName),
                        versionedPartitionName);
                }
                if (flushMap.isEmpty()) {
                    LOG.inc("takeAll>sleep>allStripes");
                    LOG.inc("takeAll>sleep>" + stripe.getName());
                    break;
                } else {
                    stripe.flush(hardFlush);
                    highwaterStorage.flush(flushMap);

                    List<Future<?>> ackFutures = new ArrayList<>();
                    for (Entry<RingMemberAndHost, Collection<AckTaken>> e : ackMap.asMap().entrySet()) {
                        futures.add(ackerThreadPool.submit(() -> {
                            try {
                                updatesTaker.ackTakenUpdate(e.getKey().ringMember, e.getKey().ringHost, e.getValue());
                            } catch (Exception x) {
                                LOG.warn("Failed to ack for " + e.getKey(), x);
                            }
                        }));
                    }
                    for (Future<?> f : ackFutures) {
                        try {
                            f.get();
                        } catch (ExecutionException x) {
                            LOG.warn("Failed to ack.", x);
                        }
                    }
                }
            } finally {
                LOG.stopTimer("takeAll>allStripes");
                LOG.stopTimer("takeAll>" + stripe.getName());
            }
        }
    }

    static class TookResult {

        public final RingMember ringMember;
        public final RingHost ringHost;
        public final VersionedPartitionName versionedPartitionName;
        public final long txId;
        public final boolean flushedAny;
        public final boolean tookFully;
        public final boolean tookError;
        public final boolean tookUnreachable;

        public TookResult(RingMember ringMember, RingHost ringHost, VersionedPartitionName versionedPartitionName,
            long txId, boolean flushedAny, boolean tookFully, boolean tookError, boolean tookUnreachable) {
            this.ringMember = ringMember;
            this.ringHost = ringHost;
            this.versionedPartitionName = versionedPartitionName;
            this.txId = txId;
            this.flushedAny = flushedAny;
            this.tookFully = tookFully;
            this.tookError = tookError;
            this.tookUnreachable = tookUnreachable;
        }

    }

    private List<TookResult> takeChanges(Entry<RingMember, RingHost>[] ring,
        PartitionStripe partitionStripe,
        VersionedPartitionName versionedPartitionName,
        int takeFromFactor) throws Exception {

        String metricName = versionedPartitionName.getPartitionName().getPartitionName() + "-" + versionedPartitionName.getPartitionName().getRingName();
        LOG.startTimer("take>all");
        LOG.startTimer("take>" + metricName);
        LOG.inc("take>all");
        LOG.inc("take>" + metricName);
        try {
            final MutableInt taken = new MutableInt(0);
            List<TookResult> tookFrom = new ArrayList<>();
            DONE:
            for (int offset = 0, loops = 0; offset < ring.length; offset = (int) Math.pow(2, loops), loops++) {
                for (int i = offset; i < ring.length; i++) {
                    if (ring[i] == null) {
                        continue;
                    }
                    Entry<RingMember, RingHost> takeFromNode = ring[i];
                    ring[i] = null;

                    RingMember ringMember = takeFromNode.getKey();
                    Long highwaterMark = highwaterStorage.get(ringMember, versionedPartitionName);
                    if (highwaterMark == null) {
                        // TODO it would be nice to ask this node to recommend an initial highwater based on
                        // TODO all of our highwaters vs. its highwater history and its start of ingress.
                        highwaterMark = -1L;
                    }
                    TakeRowStream takeRowStream = new TakeRowStream(amzaStats,
                        versionedPartitionName,
                        partitionStripe,
                        ringMember,
                        highwaterMark);

                    int updates = 0;

                    StreamingTakeResult streamingTakeResult = updatesTaker.streamingTakeUpdates(amzaRingReader.getRingMember(),
                        ringHost,
                        takeFromNode,
                        versionedPartitionName.getPartitionName(),
                        highwaterMark,
                        takeRowStream);
                    boolean tookFully = (streamingTakeResult.otherHighwaterMarks != null);

                    if (streamingTakeResult.error != null) {
                        LOG.inc("take>errors>all");
                        LOG.inc("take>errors>" + metricName);
                        if (takeFailureListener.isPresent()) {
                            takeFailureListener.get().failedToTake(takeFromNode, streamingTakeResult.error);
                        }
                        if (amzaStats.takeErrors.count(takeFromNode) == 0) {
                            LOG.warn("Error while taking from host:{}", takeFromNode);
                            LOG.trace("Error while taking from host:{} partition:{} takeFromFactor:{}",
                                new Object[]{takeFromNode, versionedPartitionName, takeFromFactor}, streamingTakeResult.error);
                        }
                        amzaStats.takeErrors.add(ringMember);
                    } else if (streamingTakeResult.unreachable != null) {
                        LOG.inc("take>unreachable>all");
                        LOG.inc("take>unreachable>" + metricName);
                        if (takeFailureListener.isPresent()) {
                            takeFailureListener.get().failedToTake(takeFromNode, streamingTakeResult.unreachable);
                        }
                        if (amzaStats.takeErrors.count(takeFromNode) == 0) {
                            LOG.debug("Unreachable while taking from host:{}", takeFromNode);
                            LOG.trace("Unreachable while taking from host:{} partition:{} takeFromFactor:{}",
                                new Object[]{takeFromNode, versionedPartitionName, takeFromFactor}, streamingTakeResult.unreachable);
                        }
                        amzaStats.takeErrors.add(ringMember);
                    } else {
                        updates = takeRowStream.flush();
                        if (tookFully) {
                            for (Entry<RingMember, Long> otherHighwaterMark : streamingTakeResult.otherHighwaterMarks.entrySet()) {
                                takeRowStream.flushedHighwatermarks.merge(otherHighwaterMark.getKey(), otherHighwaterMark.getValue(), Math::max);
                            }
                        }
                    }

                    tookFrom.add(new TookResult(takeFromNode.getKey(),
                        takeFromNode.getValue(),
                        new VersionedPartitionName(versionedPartitionName.getPartitionName(), streamingTakeResult.partitionVersion),
                        takeRowStream.largestFlushedTxId(),
                        updates > 0,
                        tookFully,
                        streamingTakeResult.error != null,
                        streamingTakeResult.unreachable != null));

                    for (Entry<RingMember, Long> entry : takeRowStream.flushedHighwatermarks.entrySet()) {
                        highwaterStorage.setIfLarger(entry.getKey(), versionedPartitionName, updates, entry.getValue());
                    }

                    if (tookFully) {
                        LOG.inc("take>fully>all");
                        LOG.inc("take>fully>" + metricName);
                        amzaStats.took(ringMember);
                        amzaStats.takeErrors.setCount(ringMember, 0);
                        if (takeFailureListener.isPresent()) {
                            takeFailureListener.get().tookFrom(takeFromNode);
                        }
                        taken.increment();
                        if (taken.intValue() >= takeFromFactor) {
                            break DONE;
                        }
                        break;
                    }
                }
            }
            return tookFrom;
        } finally {
            LOG.stopTimer("take>all");
            LOG.stopTimer("take>" + metricName);
        }

    }

    static class TakeRowStream implements RowStream {

        private final AmzaStats amzaStats;
        private final VersionedPartitionName versionedPartitionName;
        private final PartitionStripe partitionStripe;
        private final RingMember ringMember;
        private final MutableLong highWaterMark;
        private final Map<WALKey, WALValue> batch = new HashMap<>();
        private final MutableLong oldestTxId = new MutableLong(Long.MAX_VALUE);
        private final MutableLong lastTxId;
        private final MutableLong flushedTxId;
        private final AtomicInteger streamed = new AtomicInteger(0);
        private final AtomicInteger flushed = new AtomicInteger(0);
        private final AtomicReference<WALHighwater> highwater = new AtomicReference<>();
        private final BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller(); // TODO ah pass this in??
        private final BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller = new BinaryHighwaterRowMarshaller(); // TODO ah pass this in??
        private final Map<RingMember, Long> flushedHighwatermarks = new HashMap<>();

        public TakeRowStream(AmzaStats amzaStats,
            VersionedPartitionName versionedPartitionName,
            PartitionStripe partitionStripe,
            RingMember ringMember,
            long lastHighwaterMark) {
            this.amzaStats = amzaStats;
            this.versionedPartitionName = versionedPartitionName;
            this.partitionStripe = partitionStripe;
            this.ringMember = ringMember;
            this.highWaterMark = new MutableLong(lastHighwaterMark);
            this.lastTxId = new MutableLong(Long.MIN_VALUE);
            this.flushedTxId = new MutableLong(-1);
        }

        @Override
        public boolean row(long rowFP, long txId, RowType rowType, byte[] row) throws Exception {
            if (rowType == RowType.primary) {
                if (lastTxId.longValue() == Long.MIN_VALUE) {
                    lastTxId.setValue(txId);
                } else if (lastTxId.longValue() != txId) {
                    flush();
                    lastTxId.setValue(txId);
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
            flushedTxId.setValue(lastTxId.longValue());
            int numFlushed = 0;
            int batchSize = batch.size();
            if (!batch.isEmpty()) {
                amzaStats.took(ringMember, versionedPartitionName.getPartitionName(), batch.size(), oldestTxId.longValue());
                WALHighwater walh = highwater.get();
                MemoryWALUpdates updates = new MemoryWALUpdates(batch, walh);
                RowsChanged changes = partitionStripe.commit(versionedPartitionName.getPartitionName(),
                    Optional.of(versionedPartitionName.getPartitionVersion()),
                    false,
                    updates);
                if (changes != null) {
                    amzaStats.tookApplied(ringMember, versionedPartitionName.getPartitionName(), changes.getApply().size(), changes.getOldestRowTxId());
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
                    numFlushed = changes.getApply().size();
                }
            }
            highwater.set(null);
            if (batchSize > 0) {
                amzaStats.took(ringMember, versionedPartitionName.getPartitionName(), batchSize, Long.MAX_VALUE);
            }
            if (numFlushed > 0) {
                amzaStats.tookApplied(ringMember, versionedPartitionName.getPartitionName(), numFlushed, Long.MAX_VALUE);
            }
            return flushed.get();
        }

        public long largestFlushedTxId() {
            return flushedTxId.longValue();
        }
    }
}
