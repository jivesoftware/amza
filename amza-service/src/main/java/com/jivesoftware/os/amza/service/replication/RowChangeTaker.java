package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.CommitTo;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.MemoryWALUpdates;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.api.wal.WALRow;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.AmzaSystemReady;
import com.jivesoftware.os.amza.service.NotARingMemberException;
import com.jivesoftware.os.amza.service.PartitionIsExpungedException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.service.take.RowsTaker;
import com.jivesoftware.os.amza.service.take.RowsTaker.StreamingRowsResult;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import java.util.function.Predicate;
import org.apache.commons.lang.mutable.MutableLong;

import static com.jivesoftware.os.amza.service.storage.PartitionCreator.REGION_PROPERTIES;

/**
 * @author jonathan.colt
 */
public class RowChangeTaker implements RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final AmzaRingStoreReader amzaRingReader;
    private final AmzaSystemReady systemReady;
    private final RingHost ringHost;
    private final HighwaterStorage systemHighwaterStorage;
    private final RowsTaker systemRowsTaker;
    private final PartitionIndex partitionIndex;
    private final PartitionStripeProvider partitionStripeProvider;
    private final PartitionStripeFunction partitionStripeFunction;
    private final PartitionStateStorage partitionStateStorage;
    private final AvailableRowsTaker availableRowsTaker;
    private final SystemPartitionCommitChanges systemPartitionCommitChanges;
    private final StripedPartitionCommitChanges stripedPartitionCommitChanges;
    private final OrderIdProvider sessionIdProvider;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final long longPollTimeoutMillis;
    private final BinaryPrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller;

    private final ExecutorService systemRowTakerThreadPool;
    private final ExecutorService availableRowsReceiverThreadPool;
    private final ExecutorService consumerThreadPool;

    private final Object[] stripedConsumerLocks;
    private final Object systemConsumerLock = new Object();
    private final Object realignmentLock = new Object();

    private final ConcurrentHashMap<RingMember, AvailableRowsReceiver> systemAvailableRowsReceivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RingMember, AvailableRowsReceiver> stripedAvailableRowsReceivers = new ConcurrentHashMap<>();

    public RowChangeTaker(AmzaStats amzaStats,
        AmzaRingStoreReader amzaRingReader,
        AmzaSystemReady systemReady,
        RingHost ringHost,
        HighwaterStorage systemHighwaterStorage,
        RowsTaker systemRowsTaker, PartitionIndex partitionIndex,
        PartitionStripeProvider partitionStripeProvider,
        PartitionStripeFunction partitionStripeFunction,
        PartitionStateStorage partitionStateStorage,
        AvailableRowsTaker availableRowsTaker,
        SystemPartitionCommitChanges systemPartitionCommitChanges,
        StripedPartitionCommitChanges stripedPartitionCommitChanges,
        OrderIdProvider sessionIdProvider,
        Optional<TakeFailureListener> takeFailureListener,
        long longPollTimeoutMillis,
        BinaryPrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller) {

        this.amzaStats = amzaStats;
        this.amzaRingReader = amzaRingReader;
        this.systemReady = systemReady;
        this.ringHost = ringHost;
        this.systemHighwaterStorage = systemHighwaterStorage;
        this.systemRowsTaker = systemRowsTaker;
        this.partitionIndex = partitionIndex;
        this.partitionStripeProvider = partitionStripeProvider;
        this.partitionStripeFunction = partitionStripeFunction;
        this.partitionStateStorage = partitionStateStorage;
        this.availableRowsTaker = availableRowsTaker;
        this.systemPartitionCommitChanges = systemPartitionCommitChanges;
        this.stripedPartitionCommitChanges = stripedPartitionCommitChanges;
        this.sessionIdProvider = sessionIdProvider;
        this.takeFailureListener = takeFailureListener;
        this.longPollTimeoutMillis = longPollTimeoutMillis;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.binaryHighwaterRowMarshaller = binaryHighwaterRowMarshaller;

        this.systemRowTakerThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("systemRowTaker-%d").build());
        this.availableRowsReceiverThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("availableRowsReceiver-%d").build());
        this.consumerThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("availableRowsConsumer-%d").build());

        int numberOfStripes = partitionStripeFunction.getNumberOfStripes();
        this.stripedConsumerLocks = new Object[numberOfStripes];
        for (int i = 0; i < numberOfStripes; i++) {
            stripedConsumerLocks[i] = new Object();
        }
    }

    public void start() throws Exception {

        int numberOfStripes = partitionStripeFunction.getNumberOfStripes();
        scheduleConsumer(systemConsumerLock, consumerThreadPool, systemAvailableRowsReceivers, versionedPartitionName -> true);
        for (int i = 0; i < numberOfStripes; i++) {
            int stripe = i;
            scheduleConsumer(stripedConsumerLocks[i],
                consumerThreadPool,
                stripedAvailableRowsReceivers,
                versionedPartitionName -> partitionStripeFunction.stripe(versionedPartitionName.getPartitionName()) == stripe
            );
        }

        ExecutorService cya = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cya-%d").build());
        cya.submit(() -> {
            while (true) {
                try {
                    Set<RingMember> desireRingMembers = amzaRingReader.getNeighboringRingMembers(AmzaRingReader.SYSTEM_RING);
                    for (RingMember ringMember : Sets.difference(desireRingMembers, stripedAvailableRowsReceivers.keySet())) {
                        systemAvailableRowsReceivers.compute(ringMember, (key, taker) -> {
                            if (taker == null) {
                                taker = new AvailableRowsReceiver(primaryRowMarshaller, binaryHighwaterRowMarshaller, ringMember, true);
                                availableRowsReceiverThreadPool.submit(taker);
                            }
                            return taker;
                        });
                        stripedAvailableRowsReceivers.compute(ringMember, (key, taker) -> {
                            if (taker == null) {
                                taker = new AvailableRowsReceiver(primaryRowMarshaller, binaryHighwaterRowMarshaller, ringMember, false);
                                availableRowsReceiverThreadPool.submit(taker);
                            }
                            return taker;
                        });
                    }
                    for (RingMember ringMember : Sets.difference(stripedAvailableRowsReceivers.keySet(), desireRingMembers)) {
                        systemAvailableRowsReceivers.compute(ringMember, (key, taker) -> {
                            taker.dispose();
                            return null;
                        });
                        stripedAvailableRowsReceivers.compute(ringMember, (key, taker) -> {
                            taker.dispose();
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

    private Object consumerLock(PartitionName partitionName) {
        if (partitionName.isSystemPartition()) {
            return systemConsumerLock;
        } else {
            return stripedConsumerLocks[partitionStripeFunction.stripe(partitionName)];
        }
    }

    private void scheduleConsumer(Object consumerLock,
        ExecutorService consumerThreadPool,
        ConcurrentHashMap<RingMember, AvailableRowsReceiver> availableRowsReceivers,
        Predicate<VersionedPartitionName> partitionNamePredicate) throws InterruptedException {

        consumerThreadPool.submit(() -> {
            while (true) {
                boolean consumed = false;
                try {

                    for (RowChangeTaker.AvailableRowsReceiver availableRowsReceiver : availableRowsReceivers.values()) {
                        consumed |= availableRowsReceiver.consume(partitionNamePredicate);
                    }

                } catch (InterruptedException x) {
                    LOG.warn("Available rows consumer was interrupted!");
                    break;
                } catch (Exception x) {
                    LOG.error("Failed while consuming available rows.", x);
                }

                if (!consumed) {
                    synchronized (consumerLock) {
                        consumerLock.wait(1000); // TODO expose config
                    }
                }
            }

            return null;
        });
    }

    private void tookFully(VersionedAquarium versionedAquarium, RingMember remoteRingMember, long leadershipToken) throws Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            systemReady.tookFully(versionedPartitionName, remoteRingMember);
        } else {
            versionedAquarium.tookFully(remoteRingMember, leadershipToken);
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
        availableRowsReceiverThreadPool.shutdownNow();
        systemRowTakerThreadPool.shutdownNow();
        consumerThreadPool.shutdownNow();
    }

    private static class SessionedTxId {

        private final long sessionId;
        private final long txId;

        public SessionedTxId(long sessionId, long txId) {
            this.sessionId = sessionId;
            this.txId = txId;
        }
    }

    private class AvailableRowsReceiver implements Runnable {

        private final BinaryPrimaryRowMarshaller primaryRowMarshaller;
        private final BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller;
        private final RingMember remoteRingMember;
        private final boolean system;

        private final ConcurrentHashMap<VersionedPartitionName, RowTaker> versionedPartitionRowTakers = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<VersionedPartitionName, SessionedTxId> availablePartitionTxIds = new ConcurrentHashMap<>();
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        public AvailableRowsReceiver(BinaryPrimaryRowMarshaller primaryRowMarshaller,
            BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller,
            RingMember remoteRingMember,
            boolean system) {
            this.primaryRowMarshaller = primaryRowMarshaller;
            this.binaryHighwaterRowMarshaller = binaryHighwaterRowMarshaller;
            this.remoteRingMember = remoteRingMember;
            this.system = system;
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
                    amzaStats.longPolled(remoteRingMember);
                    availableRowsTaker.availableRowsStream(amzaRingReader.getRingMember(),
                        amzaRingReader.getRingHost(),
                        remoteRingMember,
                        remoteRingHost,
                        system,
                        sessionId,
                        longPollTimeoutMillis,
                        (remoteVersionedPartitionName, txId) -> {
                            amzaStats.longPollAvailables(remoteRingMember);

                            if (disposed.get()) {
                                throw new InterruptedException("MemberLatestTransactionsTaker for " + remoteRingMember + " has been disposed.");
                            }

                            PartitionName partitionName = remoteVersionedPartitionName.getPartitionName();
                            if (!amzaRingReader.isMemberOfRing(partitionName.getRingName())) {
                                LOG.info("NOT A MEMBER: local:{} remote:{} txId:{} partition:{}",
                                    ringHost, remoteRingHost, txId, remoteVersionedPartitionName);
                                return;
                            }

                            /*LOG.info("Rows available for {} on {}", remoteRingMember, remoteVersionedPartitionName);*/
                            availablePartitionTxIds.compute(remoteVersionedPartitionName, (key, existing) -> {
                                if (existing == null || sessionId > existing.sessionId || sessionId == existing.sessionId && txId > existing.txId) {
                                    return new SessionedTxId(sessionId, txId);
                                } else {
                                    return existing;
                                }
                            });

                            Object consumerLock = consumerLock(partitionName);
                            synchronized (consumerLock) {
                                consumerLock.notifyAll();
                            }
                        });
                } catch (InterruptedException ie) {
                    return;
                } catch (Exception x) {
                    if (x.getCause() instanceof InterruptedException) {
                        return;
                    }
                    LOG.error("Failed to take partitions updated:{}", new Object[] { remoteRingMember }, x);
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }
        }

        public boolean consume(Predicate<VersionedPartitionName> partitionNamePredicate) throws Exception {
            boolean consumed = false;
            Iterator<VersionedPartitionName> iter = availablePartitionTxIds.keySet().iterator();
            while (iter.hasNext()) {
                VersionedPartitionName remoteVersionedPartitionName = iter.next();
                if (partitionNamePredicate.test(remoteVersionedPartitionName)) {
                    SessionedTxId sessionedTxId = availablePartitionTxIds.remove(remoteVersionedPartitionName);
                    if (sessionedTxId != null) {
                        try {
                            consumed |= consumePartitionTxId(remoteVersionedPartitionName, sessionedTxId);
                        } catch (PropertiesNotPresentException e) {
                            LOG.info("Properties not present yet for {}", remoteVersionedPartitionName.getPartitionName());
                        } catch (PartitionIsExpungedException e) {
                            LOG.warn("Partition is expunged for {}", remoteVersionedPartitionName.getPartitionName());
                            iter.remove();
                        } catch (NotARingMemberException e) {
                            LOG.warn("Not a ring member for {}", remoteVersionedPartitionName.getPartitionName());
                            iter.remove();
                        }
                    }
                }
            }
            return consumed;
        }

        private boolean consumePartitionTxId(VersionedPartitionName remoteVersionedPartitionName, SessionedTxId sessionedTxId) throws Exception {
            RingHost remoteRingHost = amzaRingReader.getRingHost(remoteRingMember);
            PartitionName partitionName = remoteVersionedPartitionName.getPartitionName();

            ExecutorService rowTakerThreadPool;
            RowsTaker rowsTaker;
            if (partitionName.isSystemPartition()) {
                rowTakerThreadPool = systemRowTakerThreadPool;
                rowsTaker = systemRowsTaker;
            } else {
                rowTakerThreadPool = partitionStripeProvider.getRowTakerThreadPool(partitionName);
                rowsTaker = partitionStripeProvider.getRowsTaker(partitionName);
            }

            /*partitionStateStorage.remoteVersion(remoteRingMember,
             partitionName,
             remoteVersionedPartitionName.getPartitionVersion());*/
            long[] highwater = new long[1];
            VersionedPartitionName currentLocalVersionedPartitionName = partitionStateStorage.tx(partitionName, versionedAquarium -> {
                VersionedPartitionName localVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
                highwater[0] = systemHighwaterStorage.get(remoteRingMember, localVersionedPartitionName);
                PartitionStore store = partitionIndex.get(localVersionedPartitionName);
                if (store == null) {
                    //LOG.info("NO STORAGE: local:{} remote:{}  txId:{} partition:{} state:{}",
                    //    ringHost, remoteRingHost, txId, remoteVersionedPartitionName, remoteState);
                    return null;
                }
                if (livelyEndState.getCurrentState() == State.expunged) {
                    //LOG.info("EXPUNGED: local:{} remote:{}  txId:{} partition:{} localState:{} remoteState:{}",
                    //    ringHost, remoteRingHost, txId, remoteVersionedPartitionName, partitionState, remoteState);
                    return null;
                }
                if (partitionName.isSystemPartition()) {
                    if (highwater[0] >= sessionedTxId.txId) {
                        //LOG.info("NOTHING NEW: local:{} remote:{}  txId:{} partition:{} state:{}",
                        //    ringHost, remoteRingHost, txId, remoteVersionedPartitionName, remoteState);
                        return null;
                    } else {
                        return localVersionedPartitionName;
                    }
                } else {
                    return partitionStripeProvider.txPartition(partitionName, (stripe, highwaterStorage) -> {
                        if (highwater[0] >= sessionedTxId.txId && livelyEndState.isOnline()) {
                            //LOG.info("NOTHING NEW: local:{} remote:{}  txId:{} partition:{} state:{}",
                            //    ringHost, remoteRingHost, txId, remoteVersionedPartitionName, remoteState);
                            return null;
                        } else {
                            return localVersionedPartitionName;
                        }
                    });
                }
            });

            /*if (currentLocalVersionedPartitionName == null) {
                if (!partitionName.isSystemPartition()) LOG.info("PUSHBACK: local:{} remote:{} partition:{}",
                    amzaRingReader.getRingMember(), remoteRingMember, remoteVersionedPartitionName);
            } else {
                if (!partitionName.isSystemPartition()) LOG.info("AVAILABLE: local:{} remote:{} partition:{}",
                    amzaRingReader.getRingMember(), remoteRingMember, remoteVersionedPartitionName);
            }*/
            if (currentLocalVersionedPartitionName == null) {
                partitionStateStorage.tx(partitionName, versionedAquarium -> {
                    Waterline leader = versionedAquarium.getLeader();
                    long leadershipToken = (leader != null) ? leader.getTimestamp() : -1;
                    tookFully(versionedAquarium, remoteRingMember, leadershipToken);
                    return null;
                });

                rowsTaker.rowsTaken(amzaRingReader.getRingMember(),
                    remoteRingMember,
                    remoteRingHost,
                    sessionedTxId.sessionId,
                    remoteVersionedPartitionName,
                    highwater[0],
                    -1);
                return false;
            }

            //LOG.info("AVAILABLE: local:{} was told remote:{} partition:{} state:{} txId:{} is available.",
            //    ringHost, remoteRingHost, remoteVersionedPartitionName, remoteState, txId);
            versionedPartitionRowTakers.compute(remoteVersionedPartitionName, (key1, rowTaker) -> {

                if (rowTaker == null
                    || rowTaker.localVersionedPartitionName.getPartitionVersion() < currentLocalVersionedPartitionName.getPartitionVersion()) {

                    rowTaker = new RowTaker(primaryRowMarshaller,
                        binaryHighwaterRowMarshaller,
                        disposed,
                        currentLocalVersionedPartitionName,
                        remoteRingMember,
                        remoteRingHost,
                        sessionedTxId.sessionId,
                        sessionedTxId.txId,
                        remoteVersionedPartitionName,
                        rowsTaker,
                        (initialRowTaker, changed, startVersion, version) -> {
                            versionedPartitionRowTakers.computeIfPresent(remoteVersionedPartitionName, (key2, latestRowTaker) -> {
                                long initialVersion = initialRowTaker.localVersionedPartitionName.getPartitionVersion();
                                long latestVersion = latestRowTaker.localVersionedPartitionName.getPartitionVersion();
                                long latestSessionId = latestRowTaker.takeSessionId;
                                if (!disposed.get()
                                    && sessionedTxId.sessionId == latestSessionId
                                    && initialVersion == latestVersion
                                    && (changed || startVersion < version.get())) {
                                    //LOG.info("RE-SCHEDULED: local:{} take from remote:{} partition:{} state:{} txId:{}.",
                                    //    ringHost, remoteRingHost, remoteVersionedPartitionName, remoteState, txId);
                                    rowTakerThreadPool.submit(initialRowTaker);
                                    return initialRowTaker;
                                } else {
                                    //LOG.info("ALL DONE: local:{} take from remote:{} partition:{} state:{} txId:{}.",
                                    //    ringHost, remoteRingHost, remoteVersionedPartitionName, remoteState, txId);
                                    return null;
                                }
                            });
                        },
                        (_rowTaker, exception) -> {
                            rowTakerThreadPool.submit(_rowTaker);
                        });

                    //LOG.info("SCHEDULED: local:{} take from remote:{} partition:{} state:{} txId:{}.",
                    //    ringHost, remoteRingHost, remoteVersionedPartitionName, remoteState, txId);
                    rowTakerThreadPool.submit(rowTaker);
                    return rowTaker;
                } else {
                    rowTaker.moreRowsAvailable(sessionedTxId.txId);
                    return rowTaker;
                }
            });
            return true;
        }
    }

    interface OnCompletion {

        void completed(RowTaker rowTaker, boolean changed, long startVersion, AtomicLong version);
    }

    interface OnError {

        void error(RowTaker rowTaker, Exception x);
    }

    private class RowTaker implements Runnable {

        private final BinaryPrimaryRowMarshaller primaryRowMarshaller;
        private final BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller;
        private final AtomicBoolean disposed;
        private final VersionedPartitionName localVersionedPartitionName;
        private final RingMember remoteRingMember;
        private final RingHost remoteRingHost;
        private final long takeSessionId;
        private final AtomicLong takeToTxId;
        private final VersionedPartitionName remoteVersionedPartitionName;
        private final RowsTaker rowsTaker;
        private final OnCompletion onCompletion;
        private final OnError onError;

        private final AtomicLong version = new AtomicLong(0);

        public RowTaker(BinaryPrimaryRowMarshaller primaryRowMarshaller,
            BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller,
            AtomicBoolean disposed,
            VersionedPartitionName localVersionedPartitionName,
            RingMember remoteRingMember,
            RingHost remoteRingHost,
            long takeSessionId,
            long takeToTxId,
            VersionedPartitionName remoteVersionedPartitionName,
            RowsTaker rowsTaker,
            OnCompletion onCompletion,
            OnError onError) {

            this.primaryRowMarshaller = primaryRowMarshaller;
            this.binaryHighwaterRowMarshaller = binaryHighwaterRowMarshaller;
            this.disposed = disposed;
            this.localVersionedPartitionName = localVersionedPartitionName;
            this.remoteRingMember = remoteRingMember;
            this.remoteRingHost = remoteRingHost;
            this.takeSessionId = takeSessionId;
            this.takeToTxId = new AtomicLong(takeToTxId);
            this.remoteVersionedPartitionName = remoteVersionedPartitionName;
            this.rowsTaker = rowsTaker;
            this.onCompletion = onCompletion;
            this.onError = onError;
        }

        private void moreRowsAvailable(long txId) {
            //LOG.info("NUDGE: local:{}  remote:{} partition:{}.", ringHost, remoteRingHost, remoteVersionedPartitionName);
            takeToTxId.updateAndGet(existing -> Math.max(existing, txId));
            version.incrementAndGet();
        }

        @Override
        public void run() {
            if (disposed.get()) {
                return;
            }
            long currentVersion = version.get();
            PartitionName partitionName = remoteVersionedPartitionName.getPartitionName();

            try {
                //if (!remoteVersionedPartitionName.getPartitionName().isSystemPartition())
                //LOG.info("TAKE: local:{} remote:{} partition:{}.", ringHost, remoteRingHost, remoteVersionedPartitionName);
                CommitChanges commitChanges = partitionName.isSystemPartition() ? systemPartitionCommitChanges : stripedPartitionCommitChanges;
                commitChanges.commit(localVersionedPartitionName, (highwaterStorage, commitTo) -> {
                    boolean flushed = false;
                    try {

                        LOG.startTimer("take>all");
                        LOG.inc("take>all");
                        try {
                            long highwaterMark = highwaterStorage.get(remoteRingMember, localVersionedPartitionName);
//                            if (highwaterMark == null) {
//                                // TODO it would be nice to ask this node to recommend an initial highwater based on
//                                // TODO all of our highwaters vs. its highwater history and its start of ingress.
//                                highwaterMark = -1L;
//                            }

                            // TODO could avoid leadership lookup for partitions that have been configs to not care about leadership.
                            Waterline leader = partitionStateStorage.tx(partitionName, VersionedAquarium::getLeader);
                            long leadershipToken = (leader != null) ? leader.getTimestamp() : -1;
                            TakeRowStream takeRowStream = new TakeRowStream(amzaStats,
                                remoteVersionedPartitionName,
                                commitTo,
                                remoteRingMember,
                                highwaterMark,
                                primaryRowMarshaller,
                                binaryHighwaterRowMarshaller);

                            if (highwaterMark >= takeToTxId.get()) {
                                LOG.inc("take>fully>all");
                                amzaStats.took(remoteRingMember);
                                amzaStats.takeErrors.setCount(remoteRingMember, 0);
                                if (takeFailureListener.isPresent()) {
                                    takeFailureListener.get().tookFrom(remoteRingMember, remoteRingHost);
                                }
                                partitionStateStorage.tx(partitionName, versionedAquarium -> {
                                    VersionedPartitionName currentVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                                    if (currentVersionedPartitionName.getPartitionVersion() == localVersionedPartitionName.getPartitionVersion()) {
                                        tookFully(versionedAquarium, remoteRingMember, leadershipToken);
                                    }
                                    versionedAquarium.wipeTheGlass();
                                    return null;
                                });
                            } else {
                                int updates = 0;

                                StreamingRowsResult rowsResult = rowsTaker.rowsStream(amzaRingReader.getRingMember(),
                                    remoteRingMember,
                                    remoteRingHost,
                                    remoteVersionedPartitionName,
                                    highwaterMark,
                                    leadershipToken,
                                    takeRowStream);

                                if (rowsResult.error != null) {
                                    LOG.inc("take>errors>all");
                                    if (takeFailureListener.isPresent()) {
                                        takeFailureListener.get().failedToTake(remoteRingMember, remoteRingHost, rowsResult.error);
                                    }
                                    if (amzaStats.takeErrors.count(remoteRingMember) == 0) {
                                        LOG.warn("Error while taking from member:{} host:{}", remoteRingMember, remoteRingHost);
                                        LOG.trace("Error while taking from member:{} host:{} partition:{}",
                                            new Object[] { remoteRingMember, remoteRingHost, remoteVersionedPartitionName }, rowsResult.error);
                                    }
                                    amzaStats.takeErrors.add(remoteRingMember);
                                } else if (rowsResult.unreachable != null) {
                                    LOG.inc("take>unreachable>all");
                                    if (takeFailureListener.isPresent()) {
                                        takeFailureListener.get().failedToTake(remoteRingMember, remoteRingHost, rowsResult.unreachable);
                                    }
                                    if (amzaStats.takeErrors.count(remoteRingMember) == 0) {
                                        LOG.debug("Unreachable while taking from member:{} host:{}", remoteRingMember, remoteRingHost);
                                        LOG.trace("Unreachable while taking from member:{} host:{} partition:{}",
                                            new Object[] { remoteRingMember, remoteRingHost, remoteVersionedPartitionName },
                                            rowsResult.unreachable);
                                    }
                                    amzaStats.takeErrors.add(remoteRingMember);
                                } else {
                                    updates = takeRowStream.flush();
                                }

                                for (Entry<RingMember, Long> entry : takeRowStream.flushedHighwatermarks.entrySet()) {
                                    highwaterStorage.setIfLarger(entry.getKey(), localVersionedPartitionName, updates, entry.getValue());
                                }

                                if (rowsResult.partitionVersion == -1) {
                                    // Took from node in bootstrap.
                                    partitionStateStorage.tx(partitionName, versionedAquarium -> {
                                        VersionedPartitionName currentVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                                        if (currentVersionedPartitionName.getPartitionVersion() == localVersionedPartitionName.getPartitionVersion()) {
                                            LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
                                            State currentState = livelyEndState.getCurrentState();
                                            if ((currentState == State.bootstrap || currentState == null) && versionedAquarium.isColdstart()) {
                                                LOG.info("{} took {} from bootstrap {} and will coldstart with token {}", amzaRingReader.getRingMember(),
                                                    currentVersionedPartitionName,
                                                    remoteRingMember,
                                                    rowsResult.leadershipToken);
                                                tookFully(versionedAquarium, remoteRingMember, rowsResult.leadershipToken);
                                            } else {
                                                LOG.info("{} took {} from bootstrap {} but our state is {} and coldstart is {}", amzaRingReader.getRingMember(),
                                                    currentVersionedPartitionName,
                                                    remoteRingMember,
                                                    currentState,
                                                    versionedAquarium.isColdstart());
                                                versionedAquarium.wipeTheGlass();
                                            }
                                        }
                                        return null;
                                    });
                                } else if (rowsResult.otherHighwaterMarks != null) {
                                    // Other highwater are provided when taken fully.
                                    for (Entry<RingMember, Long> otherHighwaterMark : rowsResult.otherHighwaterMarks.entrySet()) {
                                        highwaterStorage.setIfLarger(otherHighwaterMark.getKey(), localVersionedPartitionName, updates,
                                            otherHighwaterMark.getValue());
                                    }

                                    LOG.inc("take>fully>all");
                                    amzaStats.took(remoteRingMember);
                                    amzaStats.takeErrors.setCount(remoteRingMember, 0);
                                    if (takeFailureListener.isPresent()) {
                                        takeFailureListener.get().tookFrom(remoteRingMember, remoteRingHost);
                                    }
                                    partitionStateStorage.tx(partitionName, versionedAquarium -> {
                                        VersionedPartitionName currentVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                                        if (currentVersionedPartitionName.getPartitionVersion() == localVersionedPartitionName.getPartitionVersion()) {
                                            tookFully(versionedAquarium, remoteRingMember, rowsResult.leadershipToken);
                                        }
                                        versionedAquarium.wipeTheGlass();
                                        return null;
                                    });
                                } else if (rowsResult.error == null) {
                                    partitionStateStorage.tx(partitionName, versionedAquarium -> {
                                        versionedAquarium.wipeTheGlass();
                                        return null;
                                    });
                                }
                                if (updates > 0) {
                                    flushed = true;
                                }
                            }
                            try {
                                //LOG.info("ACK: local:{} remote:{}  txId:{} partition:{} state:{}",
                                //    ringHost, remoteRingHost, takeRowStream.largestFlushedTxId(), remoteVersionedPartitionName);
                                rowsTaker.rowsTaken(amzaRingReader.getRingMember(),
                                    remoteRingMember,
                                    remoteRingHost,
                                    takeSessionId,
                                    remoteVersionedPartitionName,
                                    Math.max(highwaterMark, takeRowStream.largestFlushedTxId()),
                                    leadershipToken);
                            } catch (Exception x) {
                                LOG.warn("Failed to ack for member:{} host:{} partition:{}",
                                    new Object[] { remoteRingMember, remoteRingHost, remoteVersionedPartitionName }, x);
                            }
                        } finally {
                            LOG.stopTimer("take>all");
                        }

                    } catch (Exception x) {
                        LOG.warn("Failed to take from member:{} host:{} partition:{}",
                            new Object[] { remoteRingMember, remoteRingHost, localVersionedPartitionName }, x);
                    }
                    onCompletion.completed(this, flushed, currentVersion, version);
                    return null;
                });
            } catch (Exception x) {
                LOG.error("Failed to take from member:{} host:{} partition:{}",
                    new Object[] { remoteRingMember, remoteRingHost, remoteVersionedPartitionName }, x);
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
        private final List<WALRow> batch = new ArrayList<>();
        private final MutableLong oldestTxId = new MutableLong(Long.MAX_VALUE);
        private final MutableLong lastTxId;
        private final MutableLong flushedTxId;
        private final AtomicInteger streamed = new AtomicInteger(0);
        private final AtomicInteger flushed = new AtomicInteger(0);
        private final AtomicReference<WALHighwater> highwater = new AtomicReference<>();
        private final BinaryPrimaryRowMarshaller primaryRowMarshaller;
        private final BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller;
        private final Map<RingMember, Long> flushedHighwatermarks = new HashMap<>();

        public TakeRowStream(AmzaStats amzaStats,
            VersionedPartitionName versionedPartitionName,
            CommitTo commitTo,
            RingMember ringMember,
            long lastHighwaterMark,
            BinaryPrimaryRowMarshaller primaryRowMarshaller,
            BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller) {
            this.amzaStats = amzaStats;
            this.versionedPartitionName = versionedPartitionName;
            this.commitTo = commitTo;
            this.ringMember = ringMember;
            this.highWaterMark = new MutableLong(lastHighwaterMark);
            this.lastTxId = new MutableLong(Long.MIN_VALUE);
            this.primaryRowMarshaller = primaryRowMarshaller;
            this.binaryHighwaterRowMarshaller = binaryHighwaterRowMarshaller;
            this.flushedTxId = new MutableLong(-1);
        }

        @Override
        public boolean row(long rowFP, long txId, RowType rowType, byte[] row) throws Exception {
            if (rowType.isPrimary()) {
                if (lastTxId.longValue() == Long.MIN_VALUE) {
                    lastTxId.setValue(txId);
                } else if (lastTxId.longValue() != txId) {
                    flush();
                    lastTxId.setValue(txId);
                    batch.clear();
                    oldestTxId.setValue(Long.MAX_VALUE);
                }

                primaryRowMarshaller.fromRows(txFpRowStream -> txFpRowStream.stream(txId, rowFP, rowType, row),
                    (rowTxId, fp, rowType2, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, _row) -> {
                        streamed.incrementAndGet();
                        if (highWaterMark.longValue() < txId) {
                            highWaterMark.setValue(txId);
                        }
                        if (oldestTxId.longValue() > txId) {
                            oldestTxId.setValue(txId);
                        }
                        batch.add(new WALRow(rowType2, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion));
                        return true;
                    });

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
            if (!batch.isEmpty()) {
                byte[] prefix = batch.get(0).prefix; //TODO seems leaky
                amzaStats.took(ringMember, versionedPartitionName.getPartitionName(), batch.size(), oldestTxId.longValue());
                WALHighwater walh = highwater.get();
                MemoryWALUpdates updates = new MemoryWALUpdates(batch, walh);
                while (true) {
                    try {
                        RowsChanged changes = commitTo.commit(prefix, updates);
                        if (changes != null) {
                            if (walh != null) {
                                for (RingMemberHighwater memberHighwater : walh.ringMemberHighwater) {
                                    flushedHighwatermarks.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                                }
                            }
                            flushedHighwatermarks.merge(ringMember, highWaterMark.longValue(), Math::max);
                            flushed.set(streamed.get());
                            int numFlushed = changes.getApply().size();
                            if (numFlushed > 0) {
                                amzaStats.tookApplied(ringMember, versionedPartitionName.getPartitionName(), numFlushed, oldestTxId.longValue());
                            }
                        }
                        amzaStats.backPressure.set(0);
                        break;
                    } catch (DeltaOverCapacityException x) {
                        Thread.sleep(100); // TODO configure!
                        amzaStats.backPressure.incrementAndGet();
                        amzaStats.pushBacks.incrementAndGet();
                    } catch (Exception x) {
                        LOG.error("Failed while flushing.", x);
                        throw x;
                    }
                }
            }
            highwater.set(null);
            return flushed.get();
        }

        public long largestFlushedTxId() {
            return flushedTxId.longValue();
        }
    }

}
