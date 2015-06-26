package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.CommitTo;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker.StreamingTakeResult;
import com.jivesoftware.os.amza.shared.wal.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.storage.WALRow;
import com.jivesoftware.os.amza.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableLong;

import static com.jivesoftware.os.amza.service.storage.PartitionProvider.REGION_PROPERTIES;

/**
 * @author jonathan.colt
 */
public class PartitionChangeTaker implements RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private ScheduledExecutorService updateTakerThreadPool;
    private ExecutorService changeTakerThreadPool;

    private final AmzaStats amzaStats;
    private final AmzaRingStoreReader amzaRingReader;
    private final WALUpdated walUpdated;
    private final RingHost ringHost;
    private final SystemWALStorage systemWALStorage;
    private final HighwaterStorage systemHighwaterStorage;
    private final PartitionIndex partitionIndex;
    private final PartitionStripeProvider partitionStripeProvider;
    private final PartitionStatusStorage partitionStatusStorage;
    private final UpdatesTaker updatesTaker;
    private final OrderIdProvider sessionIdProvider;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final int numberOfTakerThreads;
    private final boolean hardFlush;

    private final Object realignmentLock = new Object();
    private final ConcurrentHashMap<RingMember, MemberLatestTransactionsTaker> updatedTaker = new ConcurrentHashMap<>();

    public PartitionChangeTaker(AmzaStats amzaStats,
        AmzaRingStoreReader amzaRingReader,
        WALUpdated walUpdated,
        RingHost ringHost,
        SystemWALStorage systemWALStorage,
        HighwaterStorage systemHighwaterStorage,
        PartitionIndex partitionIndex,
        PartitionStripeProvider partitionStripeProvider,
        PartitionStatusStorage partitionStatusStorage,
        UpdatesTaker updatesTaker,
        OrderIdProvider sessionIdProvider,
        Optional<TakeFailureListener> takeFailureListener,
        int numberOfTakerThreads,
        boolean hardFlush) {

        this.amzaStats = amzaStats;
        this.amzaRingReader = amzaRingReader;
        this.walUpdated = walUpdated;
        this.ringHost = ringHost;
        this.systemWALStorage = systemWALStorage;
        this.systemHighwaterStorage = systemHighwaterStorage;
        this.partitionIndex = partitionIndex;
        this.partitionStripeProvider = partitionStripeProvider;
        this.partitionStatusStorage = partitionStatusStorage;
        this.updatesTaker = updatesTaker;
        this.sessionIdProvider = sessionIdProvider;
        this.takeFailureListener = takeFailureListener;
        this.numberOfTakerThreads = numberOfTakerThreads;
        this.hardFlush = hardFlush;
    }

    public void start() throws Exception {

        if (updateTakerThreadPool == null) {
            updateTakerThreadPool = Executors.newScheduledThreadPool(numberOfTakerThreads, new ThreadFactoryBuilder().setNameFormat(
                "updatedTakeChanges-%d").build());
            changeTakerThreadPool = Executors.newFixedThreadPool(numberOfTakerThreads, new ThreadFactoryBuilder().setNameFormat(
                "changeTakeChanges-%d").build());

            ExecutorService cya = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cya-%d").build());
            cya.submit(() -> {
                while (true) {
                    try {
                        Set<RingMember> desireRingMembers = amzaRingReader.getNeighboringRingMembers(AmzaRingReader.SYSTEM_RING);
                        for (RingMember ringMember : Sets.difference(desireRingMembers, updatedTaker.keySet())) {
                            addUpdateTaker(ringMember);
                            LOG.info("Added updateTaker for ringMember:" + ringMember + " for " + amzaRingReader.getRingMember());
                        }
                        for (RingMember ringMember : Sets.difference(updatedTaker.keySet(), desireRingMembers)) {
                            updatedTaker.compute(ringMember, (key, latestTransactionsTaker) -> {
                                latestTransactionsTaker.dispose();
                                LOG.info("Removed updateTaker for ringMember:" + ringMember + " for " + amzaRingReader.getRingMember());
                                return null;
                            });
                        }

                    } catch (InterruptedException x) {
                        LOG.warn("Partition change taker CYA was interrupted!");
                        break;
                    } catch (Exception x) {
                        LOG.error("Failed while ensuring alignment.", x);
                    }

                    synchronized (realignmentLock) {
                        realignmentLock.wait(1000); // TODO expose config
                    }
                }

                return null;
            });
        }
    }

    void addUpdateTaker(RingMember ringMember) {
        updatedTaker.compute(ringMember, (RingMember t, MemberLatestTransactionsTaker taker) -> {
            if (taker == null) {
                taker = new MemberLatestTransactionsTaker(ringMember);
                updateTakerThreadPool.submit(taker);
            }
            return taker;
        });
    }

    // TODO needs to be connected.
    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (changes.getVersionedPartitionName().equals(REGION_PROPERTIES)) {
            synchronized (realignmentLock) {
                realignmentLock.notifyAll();
            }
        }
    }

    synchronized public void stop() throws Exception {
        if (updateTakerThreadPool != null) {
            this.changeTakerThreadPool.shutdownNow();
            this.changeTakerThreadPool = null;
            this.updateTakerThreadPool.shutdownNow();
            this.updateTakerThreadPool = null;
        }
    }

    private class MemberLatestTransactionsTaker implements Runnable {

        private final RingMember fromRingMember;

        private final ConcurrentHashMap<VersionedPartitionName, MemberPartitionTaker> changeTakers = new ConcurrentHashMap<>();
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        public MemberLatestTransactionsTaker(RingMember fromRingMember) {
            this.fromRingMember = fromRingMember;
        }

        public void dispose() {
            disposed.set(true);
        }

        @Override
        public void run() {
            while (!disposed.get()) {
                try {
                    RingHost fromRingHost = amzaRingReader.getRingHost(fromRingMember);
                    updatesTaker.streamingTakePartitionUpdates(amzaRingReader.getRingMember(),
                        fromRingHost,
                        sessionIdProvider.nextId(),
                        10_000, // TODO expose config
                        (remoteVersionedPartitionName, remoteStatus, txId) -> {
                            if (disposed.get()) {
                                throw new InterruptedException("MemberLatestTransactionsTaker for " + fromRingMember + " has been disposed.");
                            }

                            partitionStatusStorage.remoteStatus(fromRingMember,
                                remoteVersionedPartitionName.getPartitionName(),
                                new PartitionStatusStorage.VersionedStatus(remoteStatus, remoteVersionedPartitionName.getPartitionVersion()));

                            changeTakers.compute(remoteVersionedPartitionName, (_versionedPartitionName, taker) -> {

                                if (taker == null) {
                                    CommitChanges commitChanges;
                                    PartitionName partitionName = remoteVersionedPartitionName.getPartitionName();
                                    if (partitionName.isSystemPartition()) {
                                        commitChanges = new SystemPartitionCommitChanges(partitionName, systemWALStorage, systemHighwaterStorage, walUpdated);
                                    } else {
                                        commitChanges = new StripedPartitionCommitChanges(partitionName, partitionStripeProvider, hardFlush, walUpdated);
                                    }
                                    MemberPartitionTaker runTaker = taker;
                                    taker = new MemberPartitionTaker(disposed,
                                        fromRingMember,
                                        fromRingHost,
                                        remoteVersionedPartitionName,
                                        commitChanges,
                                        (changed, startVersion, version) -> {
                                            changeTakers.compute(remoteVersionedPartitionName, (key, memberPartitionTaker) -> {
                                                if (!disposed.get() && (changed || startVersion < version.get())) {
                                                    changeTakerThreadPool.submit(runTaker);
                                                    return memberPartitionTaker;
                                                } else {
                                                    return null;
                                                }
                                            });
                                        },
                                        (x) -> {
                                            changeTakerThreadPool.submit(runTaker);
                                        });
                                    if (taker.needsTxId(txId)) {
                                        changeTakerThreadPool.submit(taker);
                                        return taker;
                                    }
                                } else if (taker.needsTxId(txId)) {
                                    return taker;
                                }
                                return null;
                            });
                        });
                } catch (InterruptedException ie) {
                    return;
                } catch (Exception x) {
                    LOG.error("Failed to take partitions updated:{}", new Object[]{fromRingMember}, x);
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }
        }
    }

    interface OnCompletion {

        void completed(boolean changed, long startVersion, AtomicLong version);
    }

    interface OnError {

        void error(Exception x);
    }

    private class MemberPartitionTaker implements Runnable {

        private final RingMember fromRingMember;
        private final RingHost fromRingHost;
        private final VersionedPartitionName remoteVersionedPartitionName;
        private final CommitChanges commitChanges;
        private final OnCompletion onCompletion;
        private final OnError onError;

        private final AtomicBoolean disposed;
        private final AtomicLong version = new AtomicLong(0);

        public MemberPartitionTaker(AtomicBoolean disposed,
            RingMember fromRingMember,
            RingHost fromRingHost,
            VersionedPartitionName remoteVersionedPartitionName,
            CommitChanges commitChanges,
            OnCompletion onCompletion,
            OnError onError) {

            this.disposed = disposed;
            this.fromRingMember = fromRingMember;
            this.fromRingHost = fromRingHost;
            this.remoteVersionedPartitionName = remoteVersionedPartitionName;
            this.commitChanges = commitChanges;
            this.onCompletion = onCompletion;
            this.onError = onError;
        }

        private boolean needsTxId(long txId) {
            try {
                if (commitChanges.needsTxId(fromRingMember, txId)) {
                    LOG.debug("Partition:{} needs tx:{}", remoteVersionedPartitionName, txId);
                    version.incrementAndGet();
                    return true;
                }
                return false;
            } catch (Exception x) {
                throw new RuntimeException(x);
            }
        }

        @Override
        public void run() {
            if (disposed.get()) {
                return;
            }
            long currentVersion = version.get();
            PartitionName partitionName = remoteVersionedPartitionName.getPartitionName();
            LOG.startTimer("take>" + partitionName.getRingName() + ">" + partitionName.getPartitionName());
            try {

                commitChanges.commit((localVersionedPartitionName, highwaterStorage, commitTo) -> {

                    boolean flushed = false;

                    final PartitionProperties partitionProperties = partitionIndex.getProperties(localVersionedPartitionName.getPartitionName());
                    if (partitionProperties != null) {
                        if (partitionProperties.takeFromFactor > 0) {
                            try {
                                TookResult result = takeChanges(commitTo,
                                    highwaterStorage,
                                    localVersionedPartitionName,
                                    partitionProperties.takeFromFactor);

                                
                                if (result.tookFully) {
                                    partitionStatusStorage.markAsOnline(localVersionedPartitionName);
                                } else if (!result.tookError) {
                                    String ringName = localVersionedPartitionName.getPartitionName().getRingName();
                                    Set<RingMember> remoteRingMembers = amzaRingReader.getNeighboringRingMembers(ringName);
                                    partitionStatusStorage.elect(remoteRingMembers, localVersionedPartitionName);
                                }
                                if (result.flushedAny) {
                                    LOG.startTimer("takeAll>takeAck");
                                    try {
                                        updatesTaker.ackTakenUpdate(fromRingMember, fromRingHost, result.versionedPartitionName, result.txId);
                                    } catch (Exception x) {
                                        LOG.warn("Failed to ack for member:{} host:{} partition:{}",
                                            new Object[]{fromRingMember, fromRingHost, localVersionedPartitionName}, x);
                                    } finally {
                                        LOG.stopTimer("takeAll>takeAck");
                                    }
                                }
                                flushed = result.flushedAny;
                            } catch (Exception x) {
                                LOG.warn("Failed to take from member:{} host:{} partition:{}",
                                    new Object[]{fromRingMember, fromRingHost, localVersionedPartitionName}, x);
                            }
                        } else {
                            partitionStatusStorage.markAsOnline(localVersionedPartitionName);
                        }
                    }
                    onCompletion.completed(flushed, currentVersion, version);
                    return null;
                });

            } catch (Exception x) {
                LOG.error("Failed to take from member:{} host:{} partition:{}",
                    new Object[]{fromRingMember, fromRingHost, remoteVersionedPartitionName}, x);
                onError.error(x);
            } finally {
                LOG.stopTimer("take>" + partitionName.getRingName() + ">" + partitionName.getPartitionName());
            }
        }

        private TookResult takeChanges(CommitTo commitTo,
            HighwaterStorage highwaterStorage,
            VersionedPartitionName versionedPartitionName,
            int takeFromFactor) throws Exception {

            String metricName = versionedPartitionName.getPartitionName().getPartitionName() + "-" + versionedPartitionName.getPartitionName().getRingName();
            LOG.startTimer("take>all");
            LOG.startTimer("take>" + metricName);
            LOG.inc("take>all");
            LOG.inc("take>" + metricName);
            try {
                Long highwaterMark = highwaterStorage.get(fromRingMember, versionedPartitionName);
                if (highwaterMark == null) {
                    // TODO it would be nice to ask this node to recommend an initial highwater based on
                    // TODO all of our highwaters vs. its highwater history and its start of ingress.
                    highwaterMark = -1L;
                }
                TakeRowStream takeRowStream = new TakeRowStream(amzaStats,
                    versionedPartitionName,
                    commitTo,
                    fromRingMember,
                    highwaterMark);

                int updates = 0;

                StreamingTakeResult streamingTakeResult = updatesTaker.streamingTakeUpdates(amzaRingReader.getRingMember(),
                    fromRingMember,
                    fromRingHost,
                    versionedPartitionName.getPartitionName(),
                    highwaterMark,
                    takeRowStream);
                boolean tookFully = (streamingTakeResult.otherHighwaterMarks != null);

                if (streamingTakeResult.error != null) {
                    LOG.inc("take>errors>all");
                    LOG.inc("take>errors>" + metricName);
                    if (takeFailureListener.isPresent()) {
                        takeFailureListener.get().failedToTake(fromRingMember, fromRingHost, streamingTakeResult.error);
                    }
                    if (amzaStats.takeErrors.count(fromRingMember) == 0) {
                        LOG.warn("Error while taking from member:{} host:{}", fromRingMember, fromRingHost);
                        LOG.trace("Error while taking from member:{} host:{} partition:{} takeFromFactor:{}",
                            new Object[]{fromRingMember, fromRingHost, versionedPartitionName, takeFromFactor}, streamingTakeResult.error);
                    }
                    amzaStats.takeErrors.add(fromRingMember);
                } else if (streamingTakeResult.unreachable != null) {
                    LOG.inc("take>unreachable>all");
                    LOG.inc("take>unreachable>" + metricName);
                    if (takeFailureListener.isPresent()) {
                        takeFailureListener.get().failedToTake(fromRingMember, fromRingHost, streamingTakeResult.unreachable);
                    }
                    if (amzaStats.takeErrors.count(fromRingMember) == 0) {
                        LOG.debug("Unreachable while taking from member:{} host:{}", fromRingMember, fromRingHost);
                        LOG.trace("Unreachable while taking from member:{} host:{} partition:{} takeFromFactor:{}",
                            new Object[]{fromRingMember, fromRingHost, versionedPartitionName, takeFromFactor}, streamingTakeResult.unreachable);
                    }
                    amzaStats.takeErrors.add(fromRingMember);
                } else {
                    updates = takeRowStream.flush();
                    if (updates > 0) {
                        LOG.info("(" + updates + ") " + versionedPartitionName + " " + commitTo + " -> " + fromRingMember + "/" + fromRingHost);
                    }
                    if (tookFully) {
                        for (Entry<RingMember, Long> otherHighwaterMark : streamingTakeResult.otherHighwaterMarks.entrySet()) {
                            takeRowStream.flushedHighwatermarks.merge(otherHighwaterMark.getKey(), otherHighwaterMark.getValue(), Math::max);
                        }
                    }
                }

                for (Entry<RingMember, Long> entry : takeRowStream.flushedHighwatermarks.entrySet()) {
                    highwaterStorage.setIfLarger(entry.getKey(), versionedPartitionName, updates, entry.getValue());
                }

                if (tookFully) {
                    LOG.inc("take>fully>all");
                    LOG.inc("take>fully>" + metricName);
                    amzaStats.took(fromRingMember);
                    amzaStats.takeErrors.setCount(fromRingMember, 0);
                    if (takeFailureListener.isPresent()) {
                        takeFailureListener.get().tookFrom(fromRingMember, fromRingHost);
                    }
                }
                return new TookResult(fromRingMember,
                    fromRingHost,
                    new VersionedPartitionName(versionedPartitionName.getPartitionName(), streamingTakeResult.partitionVersion),
                    takeRowStream.largestFlushedTxId(),
                    updates > 0,
                    tookFully,
                    streamingTakeResult.error != null,
                    streamingTakeResult.unreachable != null);
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
                            flushedHighwatermarks.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                        }
                    }
                    flushedHighwatermarks.merge(ringMember, highWaterMark.longValue(), Math::max);
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
