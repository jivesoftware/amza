package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.ring.RingNeighbors;
import com.jivesoftware.os.amza.shared.scan.CommitTo;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

import static com.jivesoftware.os.amza.service.storage.PartitionProvider.REGION_PROPERTIES;

/**
 * @author jonathan.colt
 */
public class PartitionChangeTaker implements RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private ScheduledExecutorService updatedTakerThreadPool;
    private ExecutorService changeTakerThreadPool;
    private final AmzaRingReader amzaRingReader;
    private final RingHost ringHost;
    private final SystemWALStorage systemWALStorage;
    private final HighwaterStorage systemHighwaterStorage;

    private final AmzaStats amzaStats;
    private final PartitionIndex partitionIndex;
    private final PartitionStripeProvider partitionStripeProvider;
    private final UpdatesTaker updatesTaker;
    private final PartitionStatusStorage partitionStatusStorage;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final int numberOfTakerThreads;
    private final boolean hardFlush;

    private final Object realignmentLock = new Object();
    private final ConcurrentHashMap<PartitionName, ChangeTaker> changeTakers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RingMember, PartitionsUpdatedTaker> updatedTaker = new ConcurrentHashMap<>();

    public PartitionChangeTaker(AmzaStats amzaStats,
        AmzaRingReader amzaRingReader,
        RingHost ringHost,
        SystemWALStorage systemWALStorage,
        HighwaterStorage systemHighwaterStorage,
        PartitionIndex partitionIndex,
        PartitionStripeProvider partitionStripeProvider,
        PartitionStatusStorage partitionStatusStorage,
        UpdatesTaker updatesTaker,
        Optional<TakeFailureListener> takeFailureListener,
        int numberOfTakerThreads,
        boolean hardFlush) {

        this.amzaStats = amzaStats;
        this.amzaRingReader = amzaRingReader;
        this.ringHost = ringHost;
        this.systemWALStorage = systemWALStorage;
        this.systemHighwaterStorage = systemHighwaterStorage;
        this.partitionIndex = partitionIndex;
        this.partitionStripeProvider = partitionStripeProvider;
        this.partitionStatusStorage = partitionStatusStorage;
        this.updatesTaker = updatesTaker;
        this.takeFailureListener = takeFailureListener;
        this.numberOfTakerThreads = numberOfTakerThreads;
        this.hardFlush = hardFlush;
    }

    synchronized public void start() throws Exception {

        if (updatedTakerThreadPool == null) {
            updatedTakerThreadPool = Executors.newScheduledThreadPool(numberOfTakerThreads, new ThreadFactoryBuilder().setNameFormat(
                "updatedTakeChanges-%d").build());
            changeTakerThreadPool = Executors.newFixedThreadPool(numberOfTakerThreads, new ThreadFactoryBuilder().setNameFormat(
                "changeTakeChanges-%d").build());

            ExecutorService cya = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat(
                "cya-%d").build());
            cya.submit(() -> {
                while (true) {
                    try {
                        HashSet<PartitionName> desiredPartitionNames = new HashSet<>();
                        HashSet<RingMember> desireRingMembers = new HashSet<>();

                        for (VersionedPartitionName versionedPartitionName : partitionIndex.getAllPartitions()) {
                            desiredPartitionNames.add(versionedPartitionName.getPartitionName());
                            RingNeighbors hostRing = amzaRingReader.getRingNeighbors(versionedPartitionName.getPartitionName().getRingName());
                            for (Entry<RingMember, RingHost> host : hostRing.getAboveRing()) {
                                desireRingMembers.add(host.getKey());
                            }
                        }

                        for (RingMember ringMember : Sets.difference(desireRingMembers, updatedTaker.keySet())) {
                            addUpdateTaker(ringMember);
                            LOG.info("Added updateTaker for ringMember:" + ringMember + " for " + amzaRingReader.getRingMember());
                        }
                        for (RingMember ringMember : Sets.difference(updatedTaker.keySet(), desireRingMembers)) {
                            updatedTaker.compute(ringMember, (RingMember t, PartitionsUpdatedTaker u) -> {
                                u.dispose();
                                LOG.info("Removed updateTaker for ringMember:" + ringMember + " for " + amzaRingReader.getRingMember());
                                return null;
                            });
                        }

                        for (PartitionName partitionName : Sets.difference(desiredPartitionNames, changeTakers.keySet())) {
                            addChangeTaker(partitionName);
                            LOG.info("Added changeTaker for partitionName:" + partitionName + " for " + amzaRingReader.getRingMember());
                        }

                        for (PartitionName partitionName : Sets.difference(changeTakers.keySet(), desiredPartitionNames)) {
                            changeTakers.compute(partitionName, (PartitionName t, ChangeTaker u) -> {
                                u.dispose();
                                LOG.info("Removed changeTaker for partitionName:" + partitionName + " for " + amzaRingReader.getRingMember());
                                return null;
                            });
                        }

                    } catch (Exception x) {
                        LOG.error("Failed while ensuring aligment.");
                    }

                    synchronized (realignmentLock) {
                        realignmentLock.wait(1000); // TODO expose config
                    }
                }

            });
        }
    }

    void addUpdateTaker(RingMember ringMember) {
        updatedTaker.compute(ringMember, (RingMember t, PartitionsUpdatedTaker u) -> {
            if (u == null) {
                u = new PartitionsUpdatedTaker(ringMember, amzaRingReader, updatesTaker, changeTakerThreadPool, changeTakers);
                updatedTakerThreadPool.scheduleWithFixedDelay(u, 0, 1, TimeUnit.SECONDS);// TODO config
            }
            return u;
        });
    }

    void addChangeTaker(PartitionName partitionName) {
        changeTakers.compute(partitionName, (PartitionName t, ChangeTaker u) -> {
            if (u == null) {
                CommitChanges commitChanges;
                if (partitionName.isSystemPartition()) {
                    commitChanges = new SystemPartitionCommitChanges(new VersionedPartitionName(partitionName, 0), systemWALStorage, systemHighwaterStorage);
                } else {
                    commitChanges = new StripedPartitionCommitChanges(partitionName, partitionStripeProvider, hardFlush);
                }
                u = new ChangeTaker(amzaStats, ringHost, amzaRingReader, partitionIndex, partitionStatusStorage,
                    updatesTaker, takeFailureListener,
                    hardFlush, partitionName, commitChanges, changeTakerThreadPool);
                changeTakerThreadPool.submit(u);
            }
            return u;
        });
    }

    // TODO needs to be connected.
    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (changes.getVersionedPartitionName().getPartitionName().equals(REGION_PROPERTIES.getPartitionName())) {
            synchronized (realignmentLock) {
                realignmentLock.notifyAll();
            }
        }
    }

    synchronized public void stop() throws Exception {
        if (updatedTakerThreadPool != null) {
            this.changeTakerThreadPool.shutdownNow();
            this.changeTakerThreadPool = null;
            this.updatedTakerThreadPool.shutdownNow();
            this.updatedTakerThreadPool = null;
        }
    }

    static class PartitionsUpdatedTaker implements Runnable {

        private final RingMember ringMember;
        private final AmzaRingReader amzaRingReader;
        private final UpdatesTaker updatesTaker;

        private final ExecutorService updateTakerThreadPool;
        private final ConcurrentHashMap<PartitionName, ChangeTaker> changeTakers;
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        public PartitionsUpdatedTaker(RingMember ringMember,
            AmzaRingReader amzaRingReader,
            UpdatesTaker updatesTaker,
            ExecutorService updateTakerThreadPool,
            ConcurrentHashMap<PartitionName, ChangeTaker> changeTakers) {
            this.ringMember = ringMember;
            this.amzaRingReader = amzaRingReader;
            this.updatesTaker = updatesTaker;
            this.updateTakerThreadPool = updateTakerThreadPool;
            this.changeTakers = changeTakers;
        }

        public void dispose() {
            disposed.set(true);
        }

        @Override
        public void run() {
            try {
                if (disposed.get()) {
                    return;
                }
                RingHost ringHost = amzaRingReader.getRingHost(ringMember);
                updatesTaker.streamingTakePartitionUpdates(new AbstractMap.SimpleEntry<>(ringMember, ringHost),
                    10_000, // TODO expose config
                    (partitionName, txId) -> {

                        ChangeTaker changeTaker = changeTakers.get(partitionName);
                        if (changeTaker != null) {
                            changeTaker.awakeIfNecessary(ringMember, txId);
                        }
                        if (disposed.get()) {
                            Thread.currentThread().interrupt();
                            throw new InterruptedException("Taker has been shutdown.");
                        }

                    });
            } catch (InterruptedException ie) {
                // expected and swallowed.
            } catch (Exception x) {
                LOG.error("Failed to take partitions updated:{}", new Object[]{ringMember}, x);
                updateTakerThreadPool.submit(this);
            }
        }
    }

    static class ChangeTaker implements Runnable {

        private final AmzaStats amzaStats;
        private final RingHost ringHost;
        private final AmzaRingReader amzaRingReader;
        private final PartitionIndex partitionIndex;
        private final PartitionStatusStorage partitionStatusStorage;
        private final UpdatesTaker updatesTaker;
        private final Optional<TakeFailureListener> takeFailureListener;

        private final PartitionName partitionName;
        private final CommitChanges commitChanges;

        private final ExecutorService changeTakerThreadPool;
        private final AtomicBoolean disposed = new AtomicBoolean(false);
        private final AtomicBoolean dormant = new AtomicBoolean(false);

        public ChangeTaker(AmzaStats amzaStats,
            RingHost ringHost,
            AmzaRingReader amzaRingReader,
            PartitionIndex partitionIndex,
            PartitionStatusStorage partitionStatusStorage,
            UpdatesTaker updatesTaker,
            Optional<TakeFailureListener> takeFailureListener,
            boolean hardFlush,
            PartitionName partitionName,
            CommitChanges commitChanges,
            ExecutorService slaveTakerThreadPool) {

            this.amzaStats = amzaStats;
            this.ringHost = ringHost;
            this.amzaRingReader = amzaRingReader;
            this.partitionIndex = partitionIndex;
            this.partitionStatusStorage = partitionStatusStorage;
            this.updatesTaker = updatesTaker;
            this.takeFailureListener = takeFailureListener;
            this.partitionName = partitionName;
            this.commitChanges = commitChanges;
            this.changeTakerThreadPool = slaveTakerThreadPool;
        }

        public void dispose() {
            disposed.set(true);
        }

        @Override
        public void run() {
            if (disposed.get()) {
                return;
            }
            LOG.startTimer("take>" + partitionName.getRingName() + ">" + partitionName.getPartitionName());
            try {

                commitChanges.commit((versionedPartitionName, highwaterStorage, commitTo) -> {

                    List<RingMember> flushed = Lists.newArrayList();

                    ListMultimap<RingMemberAndHost, AckTaken> ackMap = Multimaps.synchronizedListMultimap(ArrayListMultimap
                        .<RingMemberAndHost, AckTaken>create());

                    final RingNeighbors hostRing = amzaRingReader.getRingNeighbors(versionedPartitionName.getPartitionName().getRingName());
                    final PartitionProperties partitionProperties = partitionIndex.getProperties(versionedPartitionName.getPartitionName());
                    if (partitionProperties != null) {
                        if (partitionProperties.takeFromFactor > 0) {
                            try {
                                List<TookResult> took = takeChanges(hostRing.getAboveRing(),
                                    commitTo,
                                    highwaterStorage,
                                    versionedPartitionName,
                                    partitionProperties.takeFromFactor);

                                Set<RingMember> membersUnreachable = new HashSet<>();
                                boolean allInKetchup = true;
                                boolean oneTookFully = false;
                                for (TookResult t : took) {
                                    if (t.tookFully || t.tookError) {
                                        allInKetchup = false;
                                    }
                                    if (t.tookFully) {
                                        oneTookFully = true;
                                        ackMap.put(new RingMemberAndHost(t.ringMember, t.ringHost), new AckTaken(t.versionedPartitionName,
                                            t.txId));
                                    }
                                    if (t.flushedAny) {
                                        flushed.add(t.ringMember);
                                    }
                                    if (t.tookUnreachable) {
                                        membersUnreachable.add(t.ringMember);
                                    }
                                }
                                if (allInKetchup) {
                                    partitionStatusStorage.elect(amzaRingReader.getRing(versionedPartitionName.getPartitionName().getRingName())
                                        .keySet(),
                                        membersUnreachable,
                                        versionedPartitionName);
                                }
                                if (oneTookFully) {
                                    partitionStatusStorage.markAsOnline(versionedPartitionName);
                                }
                            } catch (Exception x) {
                                LOG.warn("Failed to take from " + versionedPartitionName, x);
                            }
                        } else {
                            partitionStatusStorage.markAsOnline(versionedPartitionName);
                        }
                    }
                    if (!flushed.isEmpty()) {

                        LOG.startTimer("takeAll>takeAck");
                        try {
                            for (Entry<RingMemberAndHost, Collection<AckTaken>> e : ackMap.asMap().entrySet()) {
                                try {
                                    updatesTaker.ackTakenUpdate(e.getKey().ringMember, e.getKey().ringHost, e.getValue());
                                } catch (Exception x) {
                                    LOG.warn("Failed to ack for " + e.getKey(), x);
                                }
                            }

                        } finally {
                            LOG.stopTimer("takeAll>takeAck");
                        }
                        changeTakerThreadPool.submit(this);
                    } else {
                        //LOG.info("Dormant " + partitionName);
                        //dormant.set(true);
                        changeTakerThreadPool.submit(this);
                    }
                    return null;
                });

            } catch (Exception x) {
                LOG.error("Failed to take from:{}", new Object[]{partitionName}, x);
                changeTakerThreadPool.submit(this);
            } finally {
                LOG.stopTimer("take>" + partitionName.getRingName() + ">" + partitionName.getPartitionName());
            }
        }

        private void awakeIfNecessary(RingMember ringMember, long txId) throws Exception {
            if (commitChanges.shouldAwake(ringMember, txId)) {
                if (dormant.compareAndSet(true, false)) {
                    LOG.info("Awoke " + partitionName);
                    changeTakerThreadPool.submit(this);
                }
            }
        }

        private List<TookResult> takeChanges(Entry<RingMember, RingHost>[] ring,
            CommitTo commitTo,
            HighwaterStorage highwaterStorage,
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
                            commitTo,
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
                            if (updates > 0) {
                                LOG.info("(" + updates + ") " + partitionName + " " + commitTo + "->" + takeFromNode);
                            }
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

    static class TakeRowStream implements RowStream {

        private final AmzaStats amzaStats;
        private final VersionedPartitionName versionedPartitionName;
        private final CommitTo commitTo;
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
            CommitTo commitTo,
            RingMember ringMember,
            long lastHighwaterMark) {
            this.amzaStats = amzaStats;
            this.versionedPartitionName = versionedPartitionName;
            this.commitTo = commitTo;
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
                RowsChanged changes = commitTo.commit(updates);
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
                    if (numFlushed > 0) {

                        LOG.info(versionedPartitionName + " " + ringMember + " " + changes);
                    }
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
