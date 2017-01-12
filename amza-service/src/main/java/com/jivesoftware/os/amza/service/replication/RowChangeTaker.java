package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.partition.PartitionName;
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
import com.jivesoftware.os.amza.service.NotARingMemberException;
import com.jivesoftware.os.amza.service.PartitionIsExpungedException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.TakeFullySystemReady;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.service.take.RowsTaker;
import com.jivesoftware.os.amza.service.take.RowsTaker.StreamingRowsResult;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
    private final int numberOfStripes;
    private final StorageVersionProvider storageVersionProvider;
    private final AmzaRingStoreReader amzaRingReader;
    private final TakeFullySystemReady systemReady;
    private final RingHost ringHost;
    private final RowsTaker systemRowsTaker;
    private final RowsTaker stripedRowsTaker;
    private final PartitionStripeProvider partitionStripeProvider;
    private final AvailableRowsTaker availableRowsTaker;
    private final SystemPartitionCommitChanges systemPartitionCommitChanges;
    private final StripedPartitionCommitChanges stripedPartitionCommitChanges;
    private final OrderIdProvider sessionIdProvider;
    private final Optional<TakeFailureListener> takeFailureListener;
    private final long longPollTimeoutMillis;
    private final long pongIntervalMillis;
    private final long rowsTakerLimit;
    private final BinaryPrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller;

    private final ExecutorService systemRowTakerThreadPool;
    private final ExecutorService availableRowsReceiverThreadPool;
    private final ExecutorService consumerThreadPool;
    private final ExecutorService cyaThreadPool;

    private final ExecutorService stripedRowTakerThreadPool;

    private final Object[] stripedConsumerLocks;
    private final Object systemConsumerLock = new Object();
    private final Object realignmentLock = new Object();

    private final Map<RingMember, AvailableRowsReceiver> systemAvailableRowsReceivers = Maps.newConcurrentMap();
    private final Map<RingMember, AvailableRowsReceiver> stripedAvailableRowsReceivers = Maps.newConcurrentMap();

    public RowChangeTaker(AmzaStats amzaStats,
        int numberOfStripes,
        StorageVersionProvider storageVersionProvider,
        AmzaRingStoreReader amzaRingReader,
        TakeFullySystemReady systemReady,
        RingHost ringHost,
        RowsTaker systemRowsTaker,
        RowsTaker stripedRowsTaker,
        PartitionStripeProvider partitionStripeProvider,
        AvailableRowsTaker availableRowsTaker,
        ExecutorService rowTakerThreadPool,
        SystemPartitionCommitChanges systemPartitionCommitChanges,
        StripedPartitionCommitChanges stripedPartitionCommitChanges,
        OrderIdProvider sessionIdProvider,
        Optional<TakeFailureListener> takeFailureListener,
        long longPollTimeoutMillis,
        long pongIntervalMillis,
        long rowsTakerLimit,
        BinaryPrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller) {

        this.amzaStats = amzaStats;
        this.numberOfStripes = numberOfStripes;
        this.storageVersionProvider = storageVersionProvider;
        this.amzaRingReader = amzaRingReader;
        this.systemReady = systemReady;
        this.ringHost = ringHost;
        this.systemRowsTaker = systemRowsTaker;
        this.stripedRowsTaker = stripedRowsTaker;
        this.partitionStripeProvider = partitionStripeProvider;
        this.availableRowsTaker = availableRowsTaker;
        this.stripedRowTakerThreadPool = rowTakerThreadPool;
        this.systemPartitionCommitChanges = systemPartitionCommitChanges;
        this.stripedPartitionCommitChanges = stripedPartitionCommitChanges;
        this.sessionIdProvider = sessionIdProvider;
        this.takeFailureListener = takeFailureListener;
        this.longPollTimeoutMillis = longPollTimeoutMillis;
        this.pongIntervalMillis = pongIntervalMillis;
        this.rowsTakerLimit = rowsTakerLimit;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.binaryHighwaterRowMarshaller = binaryHighwaterRowMarshaller;

        this.systemRowTakerThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("systemRowTaker-%d").build());
        this.availableRowsReceiverThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("availableRowsReceiver-%d").build());
        this.consumerThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("availableRowsConsumer-%d").build());
        this.cyaThreadPool = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cyaThreadPool-%d").build());

        this.stripedConsumerLocks = new Object[numberOfStripes];
        for (int i = 0; i < numberOfStripes; i++) {
            stripedConsumerLocks[i] = new Object();
        }
    }

    public void start() throws Exception {

        scheduleConsumer(systemConsumerLock, consumerThreadPool, true, systemAvailableRowsReceivers, versionedPartitionName -> true);
        for (int i = 0; i < numberOfStripes; i++) {
            int consumerForStripe = i;
            scheduleConsumer(stripedConsumerLocks[i],
                consumerThreadPool,
                false,
                stripedAvailableRowsReceivers,
                versionedPartitionName -> {

                    PartitionName partitionName = versionedPartitionName.getPartitionName();
                    try {
                        return storageVersionProvider.tx(partitionName, null, (deltaIndex, stripeIndex, storageVersion) -> stripeIndex == consumerForStripe);
                    } catch (Exception x) {
                        throw new RuntimeException(x);
                    }
                }
            );
        }

        cyaThreadPool.submit(() -> {
            while (true) {
                try {
                    Set<RingMember> desireRingMembers = amzaRingReader.getNeighboringRingMembers(AmzaRingReader.SYSTEM_RING, -1);
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

    synchronized public void stop() throws Exception {
        availableRowsReceiverThreadPool.shutdownNow();
        systemRowTakerThreadPool.shutdownNow();
        consumerThreadPool.shutdownNow();
        stripedRowTakerThreadPool.shutdownNow();
        cyaThreadPool.shutdownNow();
    }

    private Object consumerLock(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return systemConsumerLock;
        } else {
            return storageVersionProvider.tx(partitionName, null, (deltaIndex, stripeIndex, storageVersion) -> {
                if (stripeIndex == -1) {
                    return null;
                } else {
                    return stripedConsumerLocks[stripeIndex];
                }
            });
        }
    }

    private void scheduleConsumer(Object consumerLock,
        ExecutorService consumerThreadPool,
        boolean system,
        Map<RingMember, AvailableRowsReceiver> availableRowsReceivers,
        Predicate<VersionedPartitionName> partitionNamePredicate) throws InterruptedException {

        consumerThreadPool.submit(() -> {
            while (true) {
                boolean consumed = false;
                try {
                    if (!system) {
                        systemReady.await(10_000L); //TODO config
                    }

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

    private static class SessionedTxId {

        private final long sessionId;
        private final String sharedKey;
        private final long txId;

        public SessionedTxId(long sessionId, String sharedKey, long txId) {
            this.sessionId = sessionId;
            this.sharedKey = sharedKey;
            this.txId = txId;
        }
    }

    private class AvailableRowsReceiver implements Runnable {

        private final BinaryPrimaryRowMarshaller primaryRowMarshaller;
        private final BinaryHighwaterRowMarshaller binaryHighwaterRowMarshaller;
        private final RingMember remoteRingMember;
        private final boolean system;

        private final AtomicLong activeSessionId = new AtomicLong(-1);
        private final AtomicReference<String> activeSharedKey = new AtomicReference<>();
        private final Map<VersionedPartitionName, RowTaker> versionedPartitionRowTakers = Maps.newConcurrentMap();
        private final Map<VersionedPartitionName, SessionedTxId> availablePartitionTxIds = Maps.newConcurrentMap();
        private final AtomicLong ping = new AtomicLong();
        private final AtomicLong pong = new AtomicLong();
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
            while (!disposed.get()) {
                try {
                    if (!system) {
                        systemReady.await(10_000L); //TODO config
                    }
                    RingHost remoteRingHost = amzaRingReader.getRingHost(remoteRingMember);
                    amzaStats.longPolled(remoteRingMember);
                    long sessionId = sessionIdProvider.nextId();
                    String sharedKey = new BigInteger(130, new SecureRandom()).toString(32);
                    activeSessionId.set(sessionId);
                    activeSharedKey.set(sharedKey);
                    availableRowsTaker.availableRowsStream(amzaRingReader.getRingMember(),
                        amzaRingReader.getRingHost(),
                        remoteRingMember,
                        remoteRingHost,
                        system,
                        sessionId,
                        sharedKey,
                        longPollTimeoutMillis,
                        (remoteVersionedPartitionName, txId) -> {
                            amzaStats.longPollAvailables(remoteRingMember);
                            // yeah yeah I hear ya
                            ping.set(System.currentTimeMillis());

                            if (disposed.get()) {
                                throw new IllegalStateException("Receiver for " + remoteRingMember + " has been disposed.");
                            }

                            PartitionName partitionName = remoteVersionedPartitionName.getPartitionName();
                            if (!amzaRingReader.isMemberOfRing(partitionName.getRingName(), system ? -1 : 0)) {
                                RowsTaker rowsTaker = system ? systemRowsTaker : stripedRowsTaker;
                                boolean invalidated = rowsTaker.invalidate(amzaRingReader.getRingMember(),
                                    remoteRingMember,
                                    remoteRingHost,
                                    sessionId,
                                    sharedKey,
                                    remoteVersionedPartitionName);
                                LOG.info("Not a member of ring invalidated:{} local:{} remote:{} txId:{} partition:{}",
                                    invalidated, ringHost, remoteRingHost, txId, remoteVersionedPartitionName);
                                return;
                            }

                            // rows to take
                            availablePartitionTxIds.compute(remoteVersionedPartitionName, (key, existing) -> {
                                if (existing == null || sessionId > existing.sessionId || sessionId == existing.sessionId && txId > existing.txId) {
                                    return new SessionedTxId(sessionId, sharedKey, txId);
                                } else {
                                    return existing;
                                }
                            });

                            Object consumerLock = consumerLock(partitionName);
                            if (consumerLock != null) {
                                synchronized (consumerLock) {
                                    consumerLock.notifyAll();
                                }
                            }
                        },
                        () -> {
                            amzaStats.pingsReceived.increment();
                            ping.set(System.currentTimeMillis());
                        });
                } catch (InterruptedException ie) {
                    return;
                } catch (Exception x) {
                    if (x.getCause() instanceof InterruptedException) {
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Failed to take partitions updated:{}", new Object[] { remoteRingMember }, x);
                    } else {
                        LOG.error("Failed to take partitions updated:{}", new Object[] { remoteRingMember }, x);
                    }
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
            consumePings();
            return consumed;
        }

        private void consumePings() throws Exception {
            RingMember localRingMember = amzaRingReader.getRingMember();
            RingHost remoteRingHost = amzaRingReader.getRingHost(remoteRingMember);
            RowsTaker rowsTaker = system ? systemRowsTaker : stripedRowsTaker;

            long sessionId = activeSessionId.get();
            if (pong.get() < ping.get() - pongIntervalMillis) {
                if (rowsTaker.pong(localRingMember, remoteRingMember, remoteRingHost, sessionId, activeSharedKey.get())) {
                    pong.set(System.currentTimeMillis());
                } else {
                    LOG.warn("Failed sending pong to member:{} session:{}", remoteRingMember, sessionId);
                }
            }
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
                rowTakerThreadPool = stripedRowTakerThreadPool;
                rowsTaker = stripedRowsTaker;
            }

            long[] highwater = { -1 };
            boolean[] validPartition = { true };

            VersionedPartitionName currentLocalVersionedPartitionName = partitionStripeProvider.txPartition(partitionName,
                (txPartitionStripe, highwaterStorage, versionedAquarium) -> {

                    VersionedPartitionName localVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                    LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
                    boolean exists = txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                        return partitionStripe == null || partitionStripe.exists(localVersionedPartitionName);
                    });
                    if (!exists) {
                        // no stripe storage
                        validPartition[0] = false;
                        return null;
                    }
                    if (livelyEndState.getCurrentState() == State.expunged) {
                        // ignore expunged
                        validPartition[0] = false;
                        return null;
                    }
                    highwater[0] = highwaterStorage.get(remoteRingMember, localVersionedPartitionName);
                    if (partitionName.isSystemPartition()) {
                        if (highwater[0] >= sessionedTxId.txId) {
                            // nothing to take
                            return null;
                        } else {
                            return localVersionedPartitionName;
                        }
                    } else if (highwater[0] >= sessionedTxId.txId && livelyEndState.isOnline()) {
                        // nothing to take
                        return null;
                    } else {
                        return localVersionedPartitionName;
                    }
                });

            if (currentLocalVersionedPartitionName == null) {
                if (validPartition[0]) {
                    partitionStripeProvider.txPartition(partitionName,
                        (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                            Waterline leader = versionedAquarium.getLeader();
                            long leadershipToken = (leader != null) ? leader.getTimestamp() : -1;
                            tookFully(versionedAquarium, remoteRingMember, leadershipToken);
                            return null;
                        });

                    rowsTaker.rowsTaken(amzaRingReader.getRingMember(),
                        remoteRingMember,
                        remoteRingHost,
                        sessionedTxId.sessionId,
                        sessionedTxId.sharedKey,
                        remoteVersionedPartitionName,
                        highwater[0],
                        -1);
                }
                return false;
            }

            // there are rows to take
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
                        sessionedTxId.sharedKey,
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
                                    // reschedule
                                    rowTakerThreadPool.submit(initialRowTaker);
                                    return initialRowTaker;
                                } else {
                                    // all done
                                    return null;
                                }
                            });
                        },
                        (_rowTaker, exception) -> {
                            rowTakerThreadPool.submit(_rowTaker);
                        });

                    // schedule the taker
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
        private final String takeSharedKey;
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
            String takeSharedKey,
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
            this.takeSharedKey = takeSharedKey;
            this.takeToTxId = new AtomicLong(takeToTxId);
            this.remoteVersionedPartitionName = remoteVersionedPartitionName;
            this.rowsTaker = rowsTaker;
            this.onCompletion = onCompletion;
            this.onError = onError;
        }

        private void moreRowsAvailable(long txId) {
            // nudge forward the txId
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
                CommitChanges commitChanges = partitionName.isSystemPartition() ? systemPartitionCommitChanges : stripedPartitionCommitChanges;
                commitChanges.commit(localVersionedPartitionName, (highwaterStorage, versionedAquarium, commitTo) -> {

                    boolean flushed = false;
                    try {

                        LOG.startTimer("take>all");
                        LOG.inc("take>all");
                        try {
                            long highwaterMark = highwaterStorage.get(remoteRingMember, localVersionedPartitionName);

                            // TODO could avoid leadership lookup for partitions that have been configs to not care about leadership.
                            Waterline leader = versionedAquarium.getLeader();
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
                                VersionedPartitionName currentVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                                if (currentVersionedPartitionName.getPartitionVersion() == localVersionedPartitionName.getPartitionVersion()) {
                                    tookFully(versionedAquarium, remoteRingMember, leadershipToken);
                                }
                                versionedAquarium.wipeTheGlass();

                            } else {
                                int updates = 0;

                                StreamingRowsResult rowsResult = rowsTaker.rowsStream(amzaRingReader.getRingMember(),
                                    remoteRingMember,
                                    remoteRingHost,
                                    remoteVersionedPartitionName,
                                    takeSessionId,
                                    takeSharedKey,
                                    highwaterMark,
                                    leadershipToken,
                                    rowsTakerLimit,
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

                                for (Entry<RingMember, DeltaIndexAndTxId> entry : takeRowStream.flushedHighwatermarks.entrySet()) {
                                    DeltaIndexAndTxId deltaIndexAndTxId = entry.getValue();
                                    highwaterStorage.setIfLarger(entry.getKey(),
                                        localVersionedPartitionName,
                                        deltaIndexAndTxId.txId,
                                        deltaIndexAndTxId.deltaIndex,
                                        updates);
                                }

                                if (rowsResult.partitionVersion == -1) {
                                    // Took from node in bootstrap.
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

                                } else if (rowsResult.otherHighwaterMarks != null) {
                                    // Other highwater are provided when taken fully.
                                    for (Entry<RingMember, Long> otherHighwaterMark : rowsResult.otherHighwaterMarks.entrySet()) {
                                        highwaterStorage.setIfLarger(otherHighwaterMark.getKey(),
                                            localVersionedPartitionName,
                                            otherHighwaterMark.getValue(),
                                            -1,
                                            0);
                                    }

                                    LOG.inc("take>fully>all");
                                    amzaStats.took(remoteRingMember);
                                    amzaStats.takeErrors.setCount(remoteRingMember, 0);
                                    if (takeFailureListener.isPresent()) {
                                        takeFailureListener.get().tookFrom(remoteRingMember, remoteRingHost);
                                    }
                                    VersionedPartitionName currentVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                                    if (currentVersionedPartitionName.getPartitionVersion() == localVersionedPartitionName.getPartitionVersion()) {
                                        tookFully(versionedAquarium, remoteRingMember, rowsResult.leadershipToken);
                                    }
                                    versionedAquarium.wipeTheGlass();

                                } else if (rowsResult.error == null) {
                                    versionedAquarium.wipeTheGlass();

                                }
                                if (updates > 0) {
                                    flushed = true;
                                }
                            }
                            try {
                                // ack rows
                                rowsTaker.rowsTaken(amzaRingReader.getRingMember(),
                                    remoteRingMember,
                                    remoteRingHost,
                                    takeSessionId,
                                    takeSharedKey,
                                    remoteVersionedPartitionName,
                                    Math.max(highwaterMark, takeRowStream.largestFlushedTxId()),
                                    leadershipToken);
                            } catch (Exception x) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Failed to ack for member:{} host:{} partition:{}",
                                        new Object[] { remoteRingMember, remoteRingHost, remoteVersionedPartitionName }, x);
                                } else {
                                    LOG.warn("Failed to ack for member:{} host:{} partition:{}",
                                        new Object[] { remoteRingMember, remoteRingHost, remoteVersionedPartitionName });
                                }

                            }
                        } finally {
                            LOG.stopTimer("take>all");
                        }

                    } catch (Exception x) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Failed to take from member:{} host:{} partition:{}",
                                new Object[] { remoteRingMember, remoteRingHost, localVersionedPartitionName }, x);
                        } else {
                            LOG.warn("Failed to take from member:{} host:{} partition:{}",
                                new Object[] { remoteRingMember, remoteRingHost, localVersionedPartitionName });
                        }
                    }
                    onCompletion.completed(this, flushed, currentVersion, version);
                    return null;
                });
            } catch (Exception x) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to take from member:{} host:{} partition:{}",
                        new Object[] { remoteRingMember, remoteRingHost, remoteVersionedPartitionName }, x);
                } else {
                    LOG.error("Failed to take from member:{} host:{} partition:{}",
                        new Object[] { remoteRingMember, remoteRingHost, remoteVersionedPartitionName });
                }
                onError.error(this, x);
            }
        }

    }

    private static class DeltaIndexAndTxId {
        private final int deltaIndex;
        private long txId = -1;

        private DeltaIndexAndTxId(int deltaIndex) {
            this.deltaIndex = deltaIndex;
        }
    }

    private static class TakeRowStream implements RowStream {

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
        private final Map<RingMember, DeltaIndexAndTxId> flushedHighwatermarks = new HashMap<>();

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
                    (rowTxId, fp, rowType2, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, _row) -> {
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
                                    mergeHighwater(changes.getDeltaIndex(), memberHighwater.ringMember, memberHighwater.transactionId);
                                }
                            }
                            mergeHighwater(changes.getDeltaIndex(), ringMember, highWaterMark.longValue());
                            flushed.set(streamed.get());
                            int numFlushed = changes.getApply().size();
                            if (numFlushed > 0) {
                                amzaStats.tookApplied(ringMember, versionedPartitionName.getPartitionName(), numFlushed, oldestTxId.longValue());
                            }
                        }
                        amzaStats.backPressure.sumThenReset();
                        break;
                    } catch (DeltaOverCapacityException x) {
                        Thread.sleep(100); // TODO configure!
                        amzaStats.backPressure.increment();
                        amzaStats.pushBacks.increment();
                    }
                }
            }
            highwater.set(null);
            return flushed.get();
        }

        private void mergeHighwater(int deltaIndex, RingMember ringMember, long transactionId) {
            DeltaIndexAndTxId deltaIndexAndTxId = flushedHighwatermarks.computeIfAbsent(ringMember, key -> new DeltaIndexAndTxId(deltaIndex));
            deltaIndexAndTxId.txId = Math.max(transactionId, deltaIndexAndTxId.txId);
        }

        public long largestFlushedTxId() {
            return flushedTxId.longValue();
        }
    }

}
