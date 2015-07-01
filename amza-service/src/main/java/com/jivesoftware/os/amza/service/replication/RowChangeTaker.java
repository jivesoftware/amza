package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.replication.PartitionStatusStorage.VersionedStatus;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
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
    private final RingHost ringHost;
    private final HighwaterStorage systemHighwaterStorage;
    private final PartitionIndex partitionIndex;
    private final PartitionStripeProvider partitionStripeProvider;
    private final PartitionStatusStorage partitionStatusStorage;
    private final RowsTaker rowsTaker;
    private final SystemPartitionCommitChanges systemPartitionCommitChanges;
    private final StripedPartitionCommitChanges stripedPartitionCommitChanges;
    private final OrderIdProvider sessionIdProvider;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final int maxRowTakerThreads;
    private final long longPollTimeoutMillis;

    private final Object realignmentLock = new Object();
    private final ConcurrentHashMap<RingMember, AvailableRows> updatedTaker = new ConcurrentHashMap<>();

    public RowChangeTaker(AmzaStats amzaStats,
        AmzaRingStoreReader amzaRingReader,
        RingHost ringHost,
        HighwaterStorage systemHighwaterStorage,
        PartitionIndex partitionIndex,
        PartitionStripeProvider partitionStripeProvider,
        PartitionStatusStorage partitionStatusStorage,
        RowsTaker rowsTaker,
        SystemPartitionCommitChanges systemPartitionCommitChanges,
        StripedPartitionCommitChanges stripedPartitionCommitChanges,
        OrderIdProvider sessionIdProvider,
        Optional<TakeFailureListener> takeFailureListener,
        int numberOfTakerThreads,
        long longPollTimeoutMillis) {

        this.amzaStats = amzaStats;
        this.amzaRingReader = amzaRingReader;
        this.ringHost = ringHost;
        this.systemHighwaterStorage = systemHighwaterStorage;
        this.partitionIndex = partitionIndex;
        this.partitionStripeProvider = partitionStripeProvider;
        this.partitionStatusStorage = partitionStatusStorage;
        this.rowsTaker = rowsTaker;
        this.systemPartitionCommitChanges = systemPartitionCommitChanges;
        this.stripedPartitionCommitChanges = stripedPartitionCommitChanges;
        this.sessionIdProvider = sessionIdProvider;
        this.takeFailureListener = takeFailureListener;
        this.maxRowTakerThreads = numberOfTakerThreads;
        this.longPollTimeoutMillis = longPollTimeoutMillis;
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
                                    //LOG.info("ADDED AvailableRows for ringMember:" + ringMember + " for " + amzaRingReader.getRingMember());
                                    availableRowThreadPool.submit(taker);
                                }
                                return taker;
                            });
                        }
                        for (RingMember ringMember : Sets.difference(updatedTaker.keySet(), desireRingMembers)) {
                            updatedTaker.compute(ringMember, (key, taker) -> {
                                taker.dispose();
                                //LOG.info("REMOVED AvailableRows for ringMember:" + ringMember + " for " + amzaRingReader.getRingMember());
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
            long sessionId = sessionIdProvider.nextId();
            while (!disposed.get()) {
                try {
                    RingHost remoteRingHost = amzaRingReader.getRingHost(remoteRingMember);
                    //LOG.info("SUBSCRIBE: local:{} -> remote:{} ", ringHost, remoteRingHost);
                    rowsTaker.availableRowsStream(amzaRingReader.getRingMember(),
                        remoteRingMember,
                        remoteRingHost,
                        sessionId,
                        longPollTimeoutMillis,
                        (remoteVersionedPartitionName, remoteStatus, txId) -> {
                            if (disposed.get()) {
                                throw new InterruptedException("MemberLatestTransactionsTaker for " + remoteRingMember + " has been disposed.");
                            }

                            PartitionName partitionName = remoteVersionedPartitionName.getPartitionName();
                            if (!amzaRingReader.isMemberOfRing(partitionName.getRingName())) {
                                //LOG.info("NOT A MEMBER: local:{} remote:{}  txId:{} partition:{} status:{}",
                                //    ringHost, remoteRingHost, txId, remoteVersionedPartitionName, remoteStatus);
                                return;
                            }

                            partitionStatusStorage.remoteStatus(remoteRingMember, partitionName,
                                new PartitionStatusStorage.VersionedStatus(remoteStatus, remoteVersionedPartitionName.getPartitionVersion()));

                            VersionedPartitionName currentLocalVersionedPartitionName = partitionStatusStorage.tx(partitionName,
                                (localVersionedPartitionName, partitionStatus) -> {
                                    if (localVersionedPartitionName == null) {
                                        VersionedStatus versionedStatus = partitionStatusStorage.markAsKetchup(partitionName);
                                        localVersionedPartitionName = new VersionedPartitionName(partitionName, versionedStatus.version);
                                        //LOG.info("FORCE KETCHUP: local:{} remote:{}  txId:{} partition:{} status:{}",
                                        //    ringHost, remoteRingHost, txId, remoteVersionedPartitionName, remoteStatus);
                                    }
                                    PartitionStore store = partitionIndex.get(localVersionedPartitionName);
                                    if (store == null) {
                                        //LOG.info("NO STORAGE: local:{} remote:{}  txId:{} partition:{} status:{}",
                                        //    ringHost, remoteRingHost, txId, remoteVersionedPartitionName, remoteStatus);
                                        return null;
                                    }
                                    if (partitionStatus != TxPartitionStatus.Status.KETCHUP && partitionStatus != TxPartitionStatus.Status.ONLINE) {
                                        //LOG.info("INVALID STATE: local:{} remote:{}  txId:{} partition:{} localStatus:{} remoteStatus:{}",
                                        //    ringHost, remoteRingHost, txId, remoteVersionedPartitionName, partitionStatus, remoteStatus);
                                        return null;
                                    }
                                    if (partitionName.isSystemPartition()) {
                                        Long highwater = systemHighwaterStorage.get(remoteRingMember, localVersionedPartitionName);
                                        if (highwater != null && highwater >= txId) {
                                            return null; // TODO ack OFFER?
                                        } else {
                                            return localVersionedPartitionName;
                                        }
                                    } else {
                                        VersionedPartitionName txLocalVersionPartitionName = localVersionedPartitionName;
                                        return partitionStripeProvider.txPartition(partitionName,
                                            (stripe, highwaterStorage) -> {
                                                Long highwater = highwaterStorage.get(remoteRingMember, txLocalVersionPartitionName);
                                                if (highwater != null && highwater >= txId) {
                                                    //LOG.info("NOTHING NEW: local:{} remote:{}  txId:{} partition:{} status:{}",
                                                    //    ringHost, remoteRingHost, txId, remoteVersionedPartitionName, remoteStatus);
                                                    return null;
                                                } else {
                                                    return txLocalVersionPartitionName;
                                                }
                                            });
                                    }
                                });
                            if (currentLocalVersionedPartitionName == null) {
                                rowsTaker.rowsTaken(remoteRingMember, remoteRingHost, remoteVersionedPartitionName, -1);
                                return;
                            }

                            //LOG.info("AVAILABLE: local:{} was told remote:{} partition:{} status:{} txId:{} is available.",
                            //    ringHost, remoteRingHost, remoteVersionedPartitionName, remoteStatus, txId);
                            versionedPartitionRowTakers.compute(remoteVersionedPartitionName, (key1, rowTaker) -> {

                                if (rowTaker == null
                                || rowTaker.localVersionedPartitionName.getPartitionVersion() < currentLocalVersionedPartitionName.getPartitionVersion()) {

                                    rowTaker = new RowTaker(disposed,
                                        currentLocalVersionedPartitionName,
                                        remoteRingMember,
                                        remoteRingHost,
                                        remoteVersionedPartitionName,
                                        (initialRowTaker, changed, startVersion, version) -> {
                                            versionedPartitionRowTakers.compute(remoteVersionedPartitionName, (key2, latestRowerTaker) -> {
                                                long initialVersion = initialRowTaker.localVersionedPartitionName.getPartitionVersion();
                                                long latestVersion = latestRowerTaker.localVersionedPartitionName.getPartitionVersion();
                                                if (!disposed.get() && initialVersion == latestVersion && (changed || startVersion < version.get())) {
                                                    //LOG.info("RE-SCHEDULED: local:{} take from remote:{} partition:{} status:{} txId:{}.",
                                                    //    ringHost, remoteRingHost, remoteVersionedPartitionName, remoteStatus, txId);
                                                    rowTakerThreadPool.submit(initialRowTaker);
                                                    return initialRowTaker;
                                                } else {
                                                    //LOG.info("ALL DONE: local:{} take from remote:{} partition:{} status:{} txId:{}.",
                                                    //    ringHost, remoteRingHost, remoteVersionedPartitionName, remoteStatus, txId);
                                                    return null;
                                                }
                                            });
                                        },
                                        (_rowTaker, exception) -> {
                                            rowTakerThreadPool.submit(_rowTaker);
                                        });

                                    //LOG.info("SCHEDULED: local:{} take from remote:{} partition:{} status:{} txId:{}.",
                                    //    ringHost, remoteRingHost, remoteVersionedPartitionName, remoteStatus, txId);
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

        private final AtomicBoolean disposed;
        private final VersionedPartitionName localVersionedPartitionName;
        private final RingMember remoteRingMember;
        private final RingHost remoteRingHost;
        private final VersionedPartitionName remoteVersionedPartitionName;
        private final OnCompletion onCompletion;
        private final OnError onError;

        private final AtomicLong version = new AtomicLong(0);

        public RowTaker(AtomicBoolean disposed,
            VersionedPartitionName localVersionedPartitionName,
            RingMember remoteRingMember,
            RingHost remoteRingHost,
            VersionedPartitionName remoteVersionedPartitionName,
            OnCompletion onCompletion,
            OnError onError) {
            this.disposed = disposed;
            this.localVersionedPartitionName = localVersionedPartitionName;
            this.remoteRingMember = remoteRingMember;
            this.remoteRingHost = remoteRingHost;
            this.remoteVersionedPartitionName = remoteVersionedPartitionName;
            this.onCompletion = onCompletion;
            this.onError = onError;
        }

        private void moreRowsMayBeAvailable() {
            //LOG.info("NUDGE: local:{}  remote:{} partition:{}.", ringHost, remoteRingHost, remoteVersionedPartitionName);
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

                //LOG.info("TAKE: local:{}  remote:{} partition:{}.", ringHost, remoteRingHost, remoteVersionedPartitionName);
                CommitChanges commitChanges = partitionName.isSystemPartition() ? systemPartitionCommitChanges : stripedPartitionCommitChanges;
                commitChanges.commit(localVersionedPartitionName, (highwaterStorage, commitTo) -> {
                    boolean flushed = false;
                    try {

                        LOG.startTimer("take>all");
                        LOG.startTimer("take>" + metricName);
                        LOG.inc("take>all");
                        LOG.inc("take>" + metricName);
                        try {
                            Long highwaterMark = highwaterStorage.get(remoteRingMember, localVersionedPartitionName);
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
                            }

                            for (Entry<RingMember, Long> entry : takeRowStream.flushedHighwatermarks.entrySet()) {
                                highwaterStorage.setIfLarger(entry.getKey(), localVersionedPartitionName, updates, entry.getValue());
                            }

                            if (rowsResult.otherHighwaterMarks != null) { // Other highwater are provide when taken fully.
                                for (Entry<RingMember, Long> otherHighwaterMark : rowsResult.otherHighwaterMarks.entrySet()) {
                                    highwaterStorage.setIfLarger(otherHighwaterMark.getKey(), localVersionedPartitionName, updates, otherHighwaterMark
                                        .getValue());
                                }

                                LOG.inc("take>fully>all");
                                LOG.inc("take>fully>" + metricName);
                                amzaStats.took(remoteRingMember);
                                amzaStats.takeErrors.setCount(remoteRingMember, 0);
                                if (takeFailureListener.isPresent()) {
                                    takeFailureListener.get().tookFrom(remoteRingMember, remoteRingHost);
                                }
                                partitionStatusStorage.markAsOnline(localVersionedPartitionName);
                            } else if (rowsResult.error == null) {
                                String ringName = localVersionedPartitionName.getPartitionName().getRingName();
                                Set<RingMember> remoteRingMembers = amzaRingReader.getNeighboringRingMembers(ringName);
                                partitionStatusStorage.elect(remoteRingMembers, localVersionedPartitionName);
                            }
                            if (updates > 0) {
                                flushed = true;
                                try {
                                    rowsTaker.rowsTaken(remoteRingMember, remoteRingHost, remoteVersionedPartitionName,
                                        takeRowStream.largestFlushedTxId());
                                } catch (Exception x) {
                                    LOG.warn("Failed to ack for member:{} host:{} partition:{}",
                                        new Object[]{remoteRingMember, remoteRingHost, remoteVersionedPartitionName}, x);
                                }
                            }

                        } finally {
                            LOG.stopTimer("take>all");
                            LOG.stopTimer("take>" + metricName);
                        }

                    } catch (Exception x) {
                        LOG.warn("Failed to take from member:{} host:{} partition:{}",
                            new Object[]{remoteRingMember, remoteRingHost, localVersionedPartitionName}, x);
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
