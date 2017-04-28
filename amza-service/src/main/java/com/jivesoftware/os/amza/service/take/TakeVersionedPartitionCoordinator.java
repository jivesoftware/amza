package com.jivesoftware.os.amza.service.take;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.StripeTx.TxPartitionStripe;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.amza.service.take.TakeCoordinator.TookLatencyStream;
import com.jivesoftware.os.amza.service.take.TakeRingCoordinator.VersionedRing;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class TakeVersionedPartitionCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    final SystemWALStorage systemWALStorage;
    final RingMember rootMember;
    final VersionedPartitionName versionedPartitionName;
    final TimestampedOrderIdProvider timestampedOrderIdProvider;
    final long slowTakeMillis;
    final long slowTakeId;
    final long systemReofferDeltaMillis;
    final long reofferDeltaMillis;
    final AtomicInteger currentCategory;
    volatile VersionedPartitionProvider.VersionedPartitionProperties versionedPartitionProperties;
    volatile boolean expunged = false;
    volatile long callCount;

    private final ConcurrentMap<RingMember, Session> sessions = Maps.newConcurrentMap();
    private final AtomicBoolean isInBootstrap = new AtomicBoolean(true);

    private long lastOfferedMillis = -1; // approximate is good enough
    private long lastTakenMillis = -1; // approximate is good enough
    private long lastCategoryCheckMillis = -1; // approximate is good enough

    public TakeVersionedPartitionCoordinator(SystemWALStorage systemWALStorage,
        RingMember rootMember,
        VersionedPartitionName versionedPartitionName,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        long slowTakeMillis,
        long slowTakeId,
        long systemReofferDeltaMillis,
        long reofferDeltaMillis) {

        this.systemWALStorage = systemWALStorage;
        this.rootMember = rootMember;
        this.versionedPartitionName = versionedPartitionName;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.systemReofferDeltaMillis = systemReofferDeltaMillis;
        this.reofferDeltaMillis = reofferDeltaMillis;
        this.slowTakeMillis = slowTakeMillis;
        this.slowTakeId = slowTakeId;
        this.currentCategory = new AtomicInteger(1);
    }

    long availableRowsStream(PartitionStripeProvider partitionStripeProvider,
        long takeSessionId,
        VersionedRing versionedRing,
        RingMember ringMember,
        AtomicLong electionCounter,
        AvailableStream availableStream) throws Exception {

        if (expunged || stableTaker(ringMember, takeSessionId, null).isDormant()) {
            return Long.MAX_VALUE;
        }

        lastOfferedMillis = System.currentTimeMillis();
        callCount++;

        return partitionStripeProvider.txPartition(versionedPartitionName.getPartitionName(),
            (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                VersionedPartitionName currentVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                Stable stable = stableTaker(ringMember, takeSessionId, versionedAquarium);
                if (stable.isDormant()) {
                    return Long.MAX_VALUE;
                } else if (currentVersionedPartitionName.getPartitionVersion() == versionedPartitionName.getPartitionVersion()) {
                    return streamHighestTxId(versionedAquarium,
                        txPartitionStripe,
                        takeSessionId,
                        versionedRing,
                        ringMember,
                        stable.isOnline(),
                        stable.isNominated(),
                        electionCounter,
                        availableStream);
                } else {
                    LOG.warn("Ignored available rows stream for invalid version {}", versionedPartitionName);
                    return Long.MAX_VALUE;
                }
            });
    }

    private long highestPartitionTx(TxPartitionStripe txPartitionStripe,
        VersionedAquarium versionedAquarium) throws Exception {

        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        if (partitionName.isSystemPartition()) {
            return systemWALStorage.highestPartitionTxId(versionedPartitionName);
        } else {
            return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                return partitionStripe.highestTxId(versionedAquarium.getVersionedPartitionName());
            });
        }
    }

    private enum Stable {
        dormant_online(true, true, false),
        active_online(false, true, false),
        active_nominated(false, true, true),
        active_offline(false, false, false);

        private final boolean dormant;
        private final boolean online;
        private final boolean nominated;

        Stable(boolean dormant, boolean online, boolean nominated) {
            this.dormant = dormant;
            this.online = online;
            this.nominated = nominated;
        }

        public boolean isDormant() {
            return dormant;
        }

        public boolean isOnline() {
            return online;
        }

        public boolean isNominated() {
            return nominated;
        }
    }

    private Stable stableTaker(RingMember ringMember, long takeSessionId, VersionedAquarium versionedAquarium) throws Exception {
        Session session = sessions.get(ringMember);
        if (session == null) {
            return Stable.active_offline;
        }
        synchronized (session) {
            if (session.sessionId != takeSessionId) {
                return Stable.active_offline;
            }

            boolean nominated = false;
            if (!session.online && versionedAquarium != null) {
                session.online = versionedAquarium.isLivelyEndState(ringMember);
                if (!session.online) {
                    nominated = versionedAquarium.isMemberInState(ringMember, State.nominated);
                }
            }
            if (session.online && session.steadyState) {
                return Stable.dormant_online;
            } else if (session.online) {
                return Stable.active_online;
            } else if (nominated) {
                return Stable.active_nominated;
            } else {
                return Stable.active_offline;
            }
        }
    }

    private long streamHighestTxId(VersionedAquarium versionedAquarium,
        TxPartitionStripe txPartitionStripe,
        long takeSessionId,
        VersionedRing versionedRing,
        RingMember ringMember,
        boolean takerIsOnline,
        boolean takerIsNominated,
        AtomicLong electionCounter,
        AvailableStream availableStream) throws Exception {

        if (isInBootstrap.get()) {
            LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
            if (livelyEndState.getCurrentState() != State.bootstrap) {
                isInBootstrap.set(false);
            }
        }

        Integer category = versionedRing.getCategory(ringMember);
        boolean isSystemPartition = versionedPartitionName.getPartitionName().isSystemPartition();
        boolean isSufficientCategory = category != null && category <= currentCategory.get();
        if (!takerIsOnline
            || isInBootstrap.get()
            || isSystemPartition
            || isSufficientCategory) {

            boolean available = false;
            long reofferDelta = (isSystemPartition ? systemReofferDeltaMillis : reofferDeltaMillis);
            long reofferAfterTimeInMillis = System.currentTimeMillis() + reofferDelta;

            Session session = sessions.computeIfAbsent(ringMember, key -> new Session());
            long highestTxId = highestPartitionTx(txPartitionStripe, versionedAquarium);
            boolean electable = false;
            while (true) {
                synchronized (session) {
                    if (session.sessionId != takeSessionId) {
                        available = true;
                        session.sessionId = takeSessionId;
                        session.offeredTxId = highestTxId;
                        session.reofferAtTimeInMillis = reofferAfterTimeInMillis;
                        session.tookTxId = -1;
                        session.tookFully = false;
                        session.steadyState = false;
                        session.online = false;
                    } else if (isSystemPartition && !session.tookFully) {
                        available = true;
                        session.sessionId = takeSessionId;
                        session.offeredTxId = highestTxId;
                        session.reofferAtTimeInMillis = reofferAfterTimeInMillis;
                        session.tookFully = false;
                        session.steadyState = false;
                    } else if (takerIsNominated || isSufficientCategory && (!takerIsOnline || shouldOffer(session, highestTxId))) {
                        electable |= electionCounter.decrementAndGet() >= 0; // note: magical short circuit OR
                        if (electable) {
                            available = true;
                            session.sessionId = takeSessionId;
                            session.offeredTxId = highestTxId;
                            session.reofferAtTimeInMillis = reofferAfterTimeInMillis;
                            session.steadyState = false;
                        } else {
                            break;
                        }
                    } else {
                        session.steadyState = !isSufficientCategory || (session.tookTxId >= highestTxId);
                    }
                }
                long checkTxId = highestPartitionTx(txPartitionStripe, versionedAquarium);
                if (checkTxId == highestTxId) {
                    break;
                } else {
                    highestTxId = checkTxId;
                }
            }
            if (available) {
                availableStream.available(versionedPartitionName, highestTxId);
                return reofferDelta;
            } else {
                return Long.MAX_VALUE;
            }
        }
        if (category == null) {
            return Long.MAX_VALUE;
        }
        return category * slowTakeMillis;
    }

    private boolean shouldOffer(Session session, long highestTxId) {
        return highestTxId > -1 && (highestTxId > session.offeredTxId
            || (highestTxId > session.tookTxId && System.currentTimeMillis() > session.reofferAtTimeInMillis));
    }

    void updateTxId(VersionedRing versionedRing, boolean replicated, long updateTxId, boolean invalidateOnline) throws Exception {
        if (expunged) {
            return;
        }

        updateCategory(versionedRing, replicated, updateTxId);

        for (Session session : sessions.values()) {
            synchronized (session) {
                session.steadyState = false;
                if (invalidateOnline) {
                    session.online = false;
                }
            }
        }
    }

    void rowsTaken(long takeSessionId,
        TxPartitionStripe txPartitionStripe,
        VersionedAquarium versionedAquarium,
        VersionedRing versionedRing,
        RingMember remoteRingMember,
        long localTxId,
        boolean replicated) throws Exception {

        lastTakenMillis = System.currentTimeMillis();

        if (expunged) {
            return;
        }

        Session session = sessions.get(remoteRingMember);
        if (session != null) {
            long highestTxId = highestPartitionTx(txPartitionStripe, versionedAquarium);
            while (true) {
                synchronized (session) {
                    if (session.sessionId == takeSessionId) {
                        long tookTxId = Math.max(localTxId, session.tookTxId);
                        session.tookTxId = tookTxId;
                        session.tookFully = (tookTxId >= session.offeredTxId);
                        session.steadyState = (localTxId >= highestTxId);
                    }
                }
                long checkTxId = highestPartitionTx(txPartitionStripe, versionedAquarium);
                if (checkTxId == highestTxId) {
                    break;
                } else {
                    highestTxId = checkTxId;
                }
            }
        }
        if (updateCategory(versionedRing, replicated, localTxId)) {
            for (Session wipe : sessions.values()) {
                synchronized (wipe) {
                    wipe.steadyState = false;
                }
            }
        }
    }

    //TODO call this?
    void cleanup(Set<RingMember> retain) {
        if (expunged) {
            return;
        }

        Set<RingMember> keySet = sessions.keySet();
        keySet.removeAll(Sets.difference(keySet, retain));
    }

    private boolean updateCategory(VersionedRing versionedRing, boolean replicated, long latestTxId) throws Exception {
        lastCategoryCheckMillis = System.currentTimeMillis();
        if (replicated) {
            long currentTimeTxId = timestampedOrderIdProvider.getApproximateId(System.currentTimeMillis());
            int[] fastEnough = { 0 };
            int worstCategory = 1;
            for (Entry<RingMember, Integer> candidate : versionedRing.members.entrySet()) {
                if (fastEnough[0] < versionedRing.takeFromFactor) {
                    worstCategory = Math.max(worstCategory, candidate.getValue());
                    Session session = sessions.get(candidate.getKey());
                    if (session != null) {
                        synchronized (session) {
                            if (session.tookTxId == latestTxId) {
                                fastEnough[0]++;
                            } else {
                                long latency = currentTimeTxId - session.tookTxId;
                                if (latency < slowTakeId * candidate.getValue()) {
                                    fastEnough[0]++;
                                }
                            }
                        }
                    }
                }
            }
            int category = currentCategory.get();
            if (category != worstCategory) {
                currentCategory.set(worstCategory);
                return true;
            }
        }
        return false;
    }

    public void expunged() {
        expunged = true;
        sessions.clear();
    }

    boolean streamTookLatencies(VersionedRing versionedRing, TookLatencyStream stream) throws Exception {
        for (Entry<RingMember, Integer> candidate : versionedRing.members.entrySet()) {
            RingMember ringMember = candidate.getKey();
            int category = candidate.getValue();
            Session session = sessions.get(ringMember);
            long tooSlowLatencyTxId = slowTakeId * category;

            if (session != null) {
                if (!stream.stream(ringMember,
                    session.offeredTxId,
                    category,
                    tooSlowLatencyTxId,
                    session.sessionId,
                    session.online,
                    session.steadyState,
                    lastOfferedMillis,
                    lastTakenMillis,
                    lastCategoryCheckMillis)) {
                    return false;
                }
            } else if (!stream.stream(ringMember,
                -1L,
                category,
                tooSlowLatencyTxId,
                -1,
                false,
                false,
                lastOfferedMillis,
                lastTakenMillis,
                lastCategoryCheckMillis)) {
                return false;
            }
        }
        return true;
    }

    public long getCallCount() {
        return callCount;
    }

    static class Session {

        long sessionId = -1;
        long offeredTxId = -1;
        long reofferAtTimeInMillis = -1;
        long tookTxId = -1;
        boolean tookFully = false;
        boolean steadyState = false;
        boolean online = false;

    }
}
