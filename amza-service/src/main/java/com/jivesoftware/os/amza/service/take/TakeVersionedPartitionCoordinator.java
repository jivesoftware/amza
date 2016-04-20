package com.jivesoftware.os.amza.service.take;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.NotARingMemberException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.StripeTx.PartitionStripePromise;
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
import java.util.concurrent.atomic.AtomicInteger;

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
        AvailableStream availableStream) throws Exception {

        if (expunged || stableTaker(ringMember, takeSessionId, null).isDormant()) {
            return Long.MAX_VALUE;
        }

        lastOfferedMillis = System.currentTimeMillis();
        callCount++;

        try {
            return partitionStripeProvider.txPartition(versionedPartitionName.getPartitionName(),
                (partitionStripePromise, highwaterStorage, versionedAquarium) -> {
                    VersionedPartitionName currentVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                    Stable stable = stableTaker(ringMember, takeSessionId, versionedAquarium);
                    if (stable.isDormant()) {
                        return Long.MAX_VALUE;
                    } else if (currentVersionedPartitionName.getPartitionVersion() == versionedPartitionName.getPartitionVersion()) {
                        long highestTxId = highestPartitionTx(partitionStripePromise, versionedAquarium);
                        return streamHighestTxId(versionedAquarium,
                            highestTxId,
                            takeSessionId,
                            versionedRing,
                            ringMember,
                            stable.isOnline(),
                            availableStream);
                    } else {
                        LOG.warn("Ignored available rows stream for invalid version {}", versionedPartitionName);
                        return Long.MAX_VALUE;
                    }
                });
        } catch (PropertiesNotPresentException e) {
            LOG.warn("Properties not present for {} when streaming available rows", versionedPartitionName);
            return Long.MAX_VALUE;
        } catch (NotARingMemberException e) {
            LOG.warn("Not a ring member for {} when streaming available rows", versionedPartitionName);
            return Long.MAX_VALUE;
        }
    }

    private long highestPartitionTx(PartitionStripePromise partitionStripePromise,
        VersionedAquarium versionedAquarium) throws Exception {

        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        if (partitionName.isSystemPartition()) {
            return systemWALStorage.highestPartitionTxId(versionedPartitionName);
        } else {
            return partitionStripePromise.get((deltaIndex, stripeIndex, partitionStripe) -> {
                return partitionStripe.highestAquariumTxId(versionedAquarium);
            });
        }
    }

    private enum Stable {
        dormant_online(true, true),
        active_online(false, true),
        active_offline(false, false);

        private final boolean dormant;
        private final boolean online;

        Stable(boolean dormant, boolean online) {
            this.dormant = dormant;
            this.online = online;
        }

        public boolean isDormant() {
            return dormant;
        }

        public boolean isOnline() {
            return online;
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

            if (!session.online && versionedAquarium != null) {
                session.online = versionedAquarium.isLivelyEndState(ringMember);
            }
            return (session.online && session.steadyState) ? Stable.dormant_online : (session.online) ? Stable.active_online : Stable.active_offline;
        }
    }

    private long streamHighestTxId(VersionedAquarium versionedAquarium,
        long highestTxId,
        long takeSessionId,
        VersionedRing versionedRing,
        RingMember ringMember,
        boolean takerIsOnline,
        AvailableStream availableStream) throws Exception {

        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();

        Integer category = versionedRing.getCategory(ringMember);
        boolean isSystemPartition = versionedPartitionName.getPartitionName().isSystemPartition();
        boolean isSufficientCategory = category != null && category <= currentCategory.get();
        if (!takerIsOnline
            || livelyEndState.getCurrentState() == State.bootstrap //TODO consider removing this check
            || isSystemPartition
            || isSufficientCategory) {

            boolean available = false;
            long reofferDelta = (isSystemPartition ? systemReofferDeltaMillis : reofferDeltaMillis);
            long reofferAfterTimeInMillis = System.currentTimeMillis() + reofferDelta;

            Session session = sessions.computeIfAbsent(ringMember, key -> new Session());
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
                } else if (isSufficientCategory && (!takerIsOnline || shouldOffer(session, highestTxId))) {
                    available = true;
                    session.sessionId = takeSessionId;
                    session.offeredTxId = highestTxId;
                    session.reofferAtTimeInMillis = reofferAfterTimeInMillis;
                    session.steadyState = false;
                } else {
                    session.steadyState = true;
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
        return highestTxId > -1 && (highestTxId > session.offeredTxId || (highestTxId > session.tookTxId && System.currentTimeMillis() > session
            .reofferAtTimeInMillis));
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
        PartitionStripePromise partitionStripePromise,
        VersionedAquarium versionedAquarium,
        VersionedRing versionedRing,
        RingMember remoteRingMember,
        long localTxId,
        boolean replicated) throws Exception {

        lastTakenMillis = System.currentTimeMillis();

        if (expunged) {
            return;
        }

        long highestTxId = highestPartitionTx(partitionStripePromise, versionedAquarium);
        Session session = sessions.get(remoteRingMember);
        if (session != null) {
            synchronized (session) {
                if (session.sessionId == takeSessionId) {
                    long tookTxId = Math.max(localTxId, session.tookTxId);
                    session.tookTxId = tookTxId;
                    session.tookFully = (tookTxId >= session.offeredTxId);
                    session.steadyState = (localTxId >= highestTxId);
                }
            }
        }
        updateCategory(versionedRing, replicated, localTxId);
    }

    //TODO call this?
    void cleanup(Set<RingMember> retain) {
        if (expunged) {
            return;
        }

        Set<RingMember> keySet = sessions.keySet();
        keySet.removeAll(Sets.difference(keySet, retain));
    }

    private void updateCategory(VersionedRing versionedRing, boolean replicated, long latestTxId) throws Exception {
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
                                long latency = currentTimeTxId - session.offeredTxId;
                                if (latency < slowTakeId * candidate.getValue()) {
                                    fastEnough[0]++;
                                }
                            }
                        }
                    }
                }/* else if (candidate.getValue() > worstCategory) {
                    sessions.remove(candidate.getKey());
                }*/
            }
            currentCategory.set(worstCategory);
        }

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

        /*public Session(long sessionId,
            long offeredTxId,
            long reofferAtTimeInMillis,
            long tookTxId,
            boolean tookFully,
            boolean steadyState,
            boolean online) {
            this.sessionId = sessionId;
            this.offeredTxId = offeredTxId;
            this.reofferAtTimeInMillis = reofferAtTimeInMillis;
            this.tookTxId = tookTxId;
            this.tookFully = tookFully;
            this.steadyState = steadyState;
            this.online = online;
        }*/
    }
}
