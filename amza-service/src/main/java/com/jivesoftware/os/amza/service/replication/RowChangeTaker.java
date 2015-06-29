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
import com.jivesoftware.os.amza.shared.take.RowsTaker;
import com.jivesoftware.os.amza.shared.take.RowsTaker.StreamingRowsResult;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableLong;

import static com.jivesoftware.os.amza.service.storage.PartitionProvider.REGION_PROPERTIES;

/**
 * @author jonathan.colt
 */
public class RowChangeTaker implements RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private ExecutorService availableRowThreadPool;
    private ExecutorService rowTakerThreadPool;

    private final AmzaStats amzaStats;
    private final AmzaRingStoreReader amzaRingReader;
    private final WALUpdated walUpdated;
    private final RingHost ringHost;
    private final SystemWALStorage systemWALStorage;
    private final HighwaterStorage systemHighwaterStorage;
    private final PartitionIndex partitionIndex;
    private final PartitionStripeProvider partitionStripeProvider;
    private final PartitionStatusStorage partitionStatusStorage;
    private final RowsTaker rowsTaker;
    private final OrderIdProvider sessionIdProvider;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final int maxRowTakerThreads;
    private final boolean hardFlush;

    private final Object realignmentLock = new Object();
    private final ConcurrentHashMap<RingMember, AvailableRows> updatedTaker = new ConcurrentHashMap<>();

    public RowChangeTaker(AmzaStats amzaStats,
        AmzaRingStoreReader amzaRingReader,
        WALUpdated walUpdated,
        RingHost ringHost,
        SystemWALStorage systemWALStorage,
        HighwaterStorage systemHighwaterStorage,
        PartitionIndex partitionIndex,
        PartitionStripeProvider partitionStripeProvider,
        PartitionStatusStorage partitionStatusStorage,
        RowsTaker updatesTaker,
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
        this.rowsTaker = updatesTaker;
        this.sessionIdProvider = sessionIdProvider;
        this.takeFailureListener = takeFailureListener;
        this.maxRowTakerThreads = numberOfTakerThreads;
        this.hardFlush = hardFlush;
    }

    public void start() throws Exception {

        if (availableRowThreadPool == null) {
            availableRowThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("availableRowThreadPool-%d").build());
            rowTakerThreadPool = Executors.newFixedThreadPool(maxRowTakerThreads, new ThreadFactoryBuilder().setNameFormat("rowTakerThreadPool-%d").build());

            ExecutorService cya = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cya-%d").build());
            cya.submit(() -> {
                while (true) {
                    try {
                        Set<RingMember> desireRingMembers = amzaRingReader.getNeighboringRingMembers(AmzaRingReader.SYSTEM_RING);
                        for (RingMember ringMember : Sets.difference(desireRingMembers, updatedTaker.keySet())) {
                            updatedTaker.compute(ringMember, (RingMember key, AvailableRows taker) -> {
                                if (taker == null) {
                                    taker = new AvailableRows(ringMember);
                                    LOG.info("ADDED AvailableRows for ringMember:" + ringMember + " for " + amzaRingReader.getRingMember());
                                    availableRowThreadPool.submit(taker);
                                }
                                return taker;
                            });
                        }
                        for (RingMember ringMember : Sets.difference(updatedTaker.keySet(), desireRingMembers)) {
                            updatedTaker.compute(ringMember, (key, taker) -> {
                                taker.dispose();
                                LOG.info("REMOVED AvailableRows for ringMember:" + ringMember + " for " + amzaRingReader.getRingMember());
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
        if (availableRowThreadPool != null) {
            this.rowTakerThreadPool.shutdownNow();
            this.rowTakerThreadPool = null;
            this.availableRowThreadPool.shutdownNow();
            this.availableRowThreadPool = null;
        }
    }

    private class AvailableRows implements Runnable {

        private final RingMember remoteRingMember;

        private final ConcurrentHashMap<VersionedPartitionName, RowTaker> versionedPartitionRowTakers = new ConcurrentHashMap<>();
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        public AvailableRows(RingMember remoteRingMember) {
            this.remoteRingMember = remoteRingMember;
        }

        public void dispose() {
            disposed.set(true);
        }

        @Override
        public void run() {
            while (!disposed.get()) {
                try {
                    RingHost remoteRingHost = amzaRingReader.getRingHost(remoteRingMember);
                    LOG.info("SUBSCRIBE: local:{} -> remote:{} ", ringHost, remoteRingHost);
                    rowsTaker.availableRowsStream(amzaRingReader.getRingMember(),
                        remoteRingMember,
                        remoteRingHost,
                        sessionIdProvider.nextId(),
                        10_000, // TODO expose config
                        (remoteVersionedPartitionName, remoteStatus, txId) -> {
                            if (disposed.get()) {
                                throw new InterruptedException("MemberLatestTransactionsTaker for " + remoteRingMember + " has been disposed.");
                            }

                            //LOG.info("AVAILABLE: local:{} was told remote:{} partition:{} status:{} txId:{} is available.",
                            //    ringHost, remoteRingHost, remoteVersionedPartitionName, remoteStatus, txId);

                            partitionStatusStorage.remoteStatus(remoteRingMember,
                                remoteVersionedPartitionName.getPartitionName(),
                                new PartitionStatusStorage.VersionedStatus(remoteStatus, remoteVersionedPartitionName.getPartitionVersion()));

                            if (remoteVersionedPartitionName.getPartitionName().isSystemPartition()) {
                                Long highwater = systemHighwaterStorage.get(remoteRingMember, remoteVersionedPartitionName);
                                if (highwater != null && highwater >= txId) {
                                    //LOG.info("ALREADY CURRENT: local:{} txId:{} vs remote:{} partition:{} status:{} txId:{}.",
                                    //    ringHost, highwater, remoteRingHost, remoteVersionedPartitionName, remoteStatus, txId);
                                    return;
                                }
                            } else {
                                if (partitionStripeProvider.txPartition(remoteVersionedPartitionName.getPartitionName(), (stripe, highwaterStorage) -> {
                                    Long highwater = highwaterStorage.get(remoteRingMember, remoteVersionedPartitionName);
                                    if (highwater != null && highwater >= txId) {
                                        //LOG.info("ALREADY CURRENT: local:{} txId:{} vs remote:{} partition:{} status:{} txId:{}.",
                                        //    ringHost, highwater, remoteRingHost, remoteVersionedPartitionName, remoteStatus, txId);
                                        return true;
                                    } else {
                                        return false;
                                    }
                                })) {
                                    return;
                                }
                            }

                            versionedPartitionRowTakers.compute(remoteVersionedPartitionName, (_versionedPartitionName, rowTaker) -> {

                                if (rowTaker == null) {
                                    CommitChanges commitChanges;
                                    PartitionName partitionName = remoteVersionedPartitionName.getPartitionName();
                                    if (partitionName.isSystemPartition()) {
                                        commitChanges = new SystemPartitionCommitChanges(partitionName, systemWALStorage, systemHighwaterStorage, walUpdated);
                                    } else {
                                        commitChanges = new StripedPartitionCommitChanges(partitionName, partitionStripeProvider, hardFlush, walUpdated);
                                    }

                                    rowTaker = new RowTaker(disposed,
                                        remoteRingMember,
                                        remoteRingHost,
                                        remoteVersionedPartitionName,
                                        commitChanges,
                                        (_rowTaker, changed, startVersion, version) -> {
                                            versionedPartitionRowTakers.compute(remoteVersionedPartitionName, (key, memberPartitionTaker) -> {
                                                if (!disposed.get() && (changed || startVersion < version.get())) {
                                                    rowTakerThreadPool.submit(_rowTaker);
                                                    return memberPartitionTaker;
                                                } else {
                                                    return null;
                                                }
                                            });
                                        },
                                        (_rowTaker, exception) -> {
                                            rowTakerThreadPool.submit(_rowTaker);
                                        });

                                    LOG.info("SCHEDUALED: local:{} take from remote:{} partition:{} status:{} txId:{}.",
                                        ringHost, remoteRingHost, remoteVersionedPartitionName, remoteStatus, txId);
                                    rowTakerThreadPool.submit(rowTaker);
                                    return rowTaker;
                                } else {
                                    rowTaker.moreRowsMayBeAvailable();
                                    return rowTaker;
                                }
                            });

                        });
                } catch (InterruptedException ie) {
                    return;
                } catch (Exception x) {
                    LOG.error("Failed to take partitions updated:{}", new Object[]{remoteRingMember}, x);
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

        void completed(RowTaker rowTaker, boolean changed, long startVersion, AtomicLong version);
    }

    interface OnError {

        void error(RowTaker rowTaker, Exception x);
    }

    private class RowTaker implements Runnable {

        private final RingMember remoteRingMember;
        private final RingHost remoteRingHost;
        private final VersionedPartitionName remoteVersionedPartitionName;
        private final CommitChanges commitChanges;
        private final OnCompletion onCompletion;
        private final OnError onError;

        private final AtomicBoolean disposed;
        private final AtomicLong version = new AtomicLong(0);

        public RowTaker(AtomicBoolean disposed,
            RingMember remoteRingMember,
            RingHost remoteRingHost,
            VersionedPartitionName remoteVersionedPartitionName,
            CommitChanges commitChanges,
            OnCompletion onCompletion,
            OnError onError) {

            this.disposed = disposed;
            this.remoteRingMember = remoteRingMember;
            this.remoteRingHost = remoteRingHost;
            this.remoteVersionedPartitionName = remoteVersionedPartitionName;
            this.commitChanges = commitChanges;
            this.onCompletion = onCompletion;
            this.onError = onError;
        }

        private void moreRowsMayBeAvailable() {
            version.incrementAndGet();
        }

        @Override
        public void run() {
            if (disposed.get()) {
                return;
            }
            long currentVersion = version.get();
            PartitionName partitionName = remoteVersionedPartitionName.getPartitionName();
            String metricName = partitionName.getName() + "-" + partitionName.getRingName();
            try {

                LOG.info("TAKE: local:{}  remote:{} partition:{}.", ringHost, remoteRingHost, remoteVersionedPartitionName);

                commitChanges.commit((localVersionedPartitionName, highwaterStorage, commitTo) -> {

                    boolean flushed = false;

                    final PartitionProperties partitionProperties = partitionIndex.getProperties(localVersionedPartitionName.getPartitionName());
                    if (partitionProperties != null) {
                        if (partitionProperties.takeFromFactor > 0) {
                            try {
//                                TookResult result = takeChanges(commitTo,
//                                    highwaterStorage,
//                                    localVersionedPartitionName,
//                                    partitionProperties.takeFromFactor);
//

                                LOG.startTimer("take>all");
                                LOG.startTimer("take>" + metricName);
                                LOG.inc("take>all");
                                LOG.inc("take>" + metricName);
                                try {
                                    Long highwaterMark = highwaterStorage.get(remoteRingMember, remoteVersionedPartitionName);
                                    if (highwaterMark == null) {
                                        // TODO it would be nice to ask this node to recommend an initial highwater based on
                                        // TODO all of our highwaters vs. its highwater history and its start of ingress.
                                        highwaterMark = -1L;
                                    }
                                    TakeRowStream takeRowStream = new TakeRowStream(amzaStats,
                                        remoteVersionedPartitionName,
                                        commitTo,
                                        remoteRingMember,
                                        highwaterMark);

                                    int updates = 0;

                                    StreamingRowsResult rowsResult = rowsTaker.rowsStream(amzaRingReader.getRingMember(),
                                        remoteRingMember,
                                        remoteRingHost,
                                        remoteVersionedPartitionName,
                                        highwaterMark,
                                        takeRowStream);
                                    boolean tookFully = (rowsResult.otherHighwaterMarks != null);

                                    if (rowsResult.error != null) {
                                        LOG.inc("take>errors>all");
                                        LOG.inc("take>errors>" + metricName);
                                        if (takeFailureListener.isPresent()) {
                                            takeFailureListener.get().failedToTake(remoteRingMember, remoteRingHost, rowsResult.error);
                                        }
                                        if (amzaStats.takeErrors.count(remoteRingMember) == 0) {
                                            LOG.warn("Error while taking from member:{} host:{}", remoteRingMember, remoteRingHost);
                                            LOG.trace("Error while taking from member:{} host:{} partition:{}",
                                                new Object[]{remoteRingMember, remoteRingHost, remoteVersionedPartitionName}, rowsResult.error);
                                        }
                                        amzaStats.takeErrors.add(remoteRingMember);
                                    } else if (rowsResult.unreachable != null) {
                                        LOG.inc("take>unreachable>all");
                                        LOG.inc("take>unreachable>" + metricName);
                                        if (takeFailureListener.isPresent()) {
                                            takeFailureListener.get().failedToTake(remoteRingMember, remoteRingHost, rowsResult.unreachable);
                                        }
                                        if (amzaStats.takeErrors.count(remoteRingMember) == 0) {
                                            LOG.debug("Unreachable while taking from member:{} host:{}", remoteRingMember, remoteRingHost);
                                            LOG.trace("Unreachable while taking from member:{} host:{} partition:{}",
                                                new Object[]{remoteRingMember, remoteRingHost, remoteVersionedPartitionName},
                                                rowsResult.unreachable);
                                        }
                                        amzaStats.takeErrors.add(remoteRingMember);
                                    } else {
                                        updates = takeRowStream.flush();
                                        if (updates > 0) {
                                            LOG.info("APPLIED: ({}) {} {} -> {}/{}",
                                                updates, remoteVersionedPartitionName, commitTo, remoteRingMember, remoteRingHost);
                                        } else {
                                            LOG.info("EMPTY TAKE: ({}) {} {} -> {}/{}",
                                                updates, remoteVersionedPartitionName, commitTo, remoteRingMember, remoteRingHost);
                                        }
                                        if (tookFully) {
                                            for (Entry<RingMember, Long> otherHighwaterMark : rowsResult.otherHighwaterMarks.entrySet()) {
                                                takeRowStream.flushedHighwatermarks.merge(otherHighwaterMark.getKey(), otherHighwaterMark.getValue(), Math::max);
                                            }
                                        }
                                    }

                                    for (Entry<RingMember, Long> entry : takeRowStream.flushedHighwatermarks.entrySet()) {
                                        highwaterStorage.setIfLarger(entry.getKey(), remoteVersionedPartitionName, updates, entry.getValue());
                                    }

                                    if (tookFully) {
                                        LOG.inc("take>fully>all");
                                        LOG.inc("take>fully>" + metricName);
                                        amzaStats.took(remoteRingMember);
                                        amzaStats.takeErrors.setCount(remoteRingMember, 0);
                                        if (takeFailureListener.isPresent()) {
                                            takeFailureListener.get().tookFrom(remoteRingMember, remoteRingHost);
                                        }
                                    }

                                   
                                    if (tookFully) {
                                        partitionStatusStorage.markAsOnline(localVersionedPartitionName);
                                    } else if (rowsResult.error == null) {
                                        String ringName = localVersionedPartitionName.getPartitionName().getRingName();
                                        Set<RingMember> remoteRingMembers = amzaRingReader.getNeighboringRingMembers(ringName);
                                        partitionStatusStorage.elect(remoteRingMembers, localVersionedPartitionName);
                                    }
                                    if (updates > 0) {
                                        LOG.startTimer("takeAll>takeAck");
                                        try {
                                            rowsTaker.rowsTaken(remoteRingMember, remoteRingHost, remoteVersionedPartitionName, takeRowStream
                                                .largestFlushedTxId());
                                        } catch (Exception x) {
                                            LOG.warn("Failed to ack for member:{} host:{} partition:{}",
                                                new Object[]{remoteRingMember, remoteRingHost, localVersionedPartitionName}, x);
                                        } finally {
                                            LOG.stopTimer("takeAll>takeAck");
                                        }
                                    }
                                    flushed = updates > 0;

                                } finally {
                                    LOG.stopTimer("take>all");
                                    LOG.stopTimer("take>" + metricName);
                                }

                            } catch (Exception x) {
                                LOG.warn("Failed to take from member:{} host:{} partition:{}",
                                    new Object[]{remoteRingMember, remoteRingHost, localVersionedPartitionName}, x);
                            }
                        } else {
                            partitionStatusStorage.markAsOnline(localVersionedPartitionName);
                        }
                    }
                    onCompletion.completed(this, flushed, currentVersion, version);
                    return null;
                });

            } catch (Exception x) {
                LOG.error("Failed to take from member:{} host:{} partition:{}",
                    new Object[]{remoteRingMember, remoteRingHost, remoteVersionedPartitionName}, x);
                onError.error(this, x);
            }
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
