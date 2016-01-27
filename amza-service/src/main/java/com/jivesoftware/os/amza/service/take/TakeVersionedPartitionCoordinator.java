package com.jivesoftware.os.amza.service.take;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.NotARingMemberException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.partition.TxHighestPartitionTx;
import com.jivesoftware.os.amza.service.replication.PartitionStateStorage;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan.colt
 */
public class TakeVersionedPartitionCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    final RingMember rootMember;
    final VersionedPartitionName versionedPartitionName;
    final TimestampedOrderIdProvider timestampedOrderIdProvider;
    final long slowTakeMillis;
    final long slowTakeId;
    final long systemReofferDeltaMillis;
    final long reofferDeltaMillis;
    final AtomicInteger currentCategory;
    final AtomicBoolean expunged = new AtomicBoolean(false);

    private final ConcurrentHashMap<RingMember, SessionedTxId> took = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RingMember, Long> steadyState = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RingMember, Long> onlineTakers = new ConcurrentHashMap<>();

    public TakeVersionedPartitionCoordinator(RingMember rootMember,
        VersionedPartitionName versionedPartitionName,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        long slowTakeMillis,
        long slowTakeId,
        long systemReofferDeltaMillis,
        long reofferDeltaMillis) {

        this.rootMember = rootMember;
        this.versionedPartitionName = versionedPartitionName;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.systemReofferDeltaMillis = systemReofferDeltaMillis;
        this.reofferDeltaMillis = reofferDeltaMillis;
        this.slowTakeMillis = slowTakeMillis;
        this.slowTakeId = slowTakeId;
        this.currentCategory = new AtomicInteger(1);
    }

    private boolean isSteadyState(RingMember ringMember, long takeSessionId) throws Exception {
        Long steadySessionId = steadyState.get(ringMember);
        return steadySessionId != null && steadySessionId == takeSessionId;
    }

    Long availableRowsStream(PartitionStateStorage partitionStateStorage,
        TxHighestPartitionTx<Long> txHighestPartitionTx,
        long takeSessionId,
        VersionedRing versionedRing,
        RingMember ringMember,
        int takeFromFactor,
        AvailableStream availableStream) throws Exception {

        if (!expunged.get() && takeFromFactor > 0) {
            synchronized (steadyState) {
                try {
                    return partitionStateStorage.tx(versionedPartitionName.getPartitionName(), versionedAquarium -> {
                        VersionedPartitionName currentVersionedPartitionName = versionedAquarium.getVersionedPartitionName();
                        boolean takerIsOnline = takerIsOnline(ringMember, takeSessionId, versionedAquarium);
                        if (takerIsOnline && isSteadyState(ringMember, takeSessionId)) {
                            return Long.MAX_VALUE;
                        } else if (currentVersionedPartitionName.getPartitionVersion() == versionedPartitionName.getPartitionVersion()) {
                            return txHighestPartitionTx.tx(versionedAquarium, (versionedAquarium1, highestTxId) -> {
                                if (versionedAquarium1 != null) {
                                    return streamHighestTxId(versionedAquarium1,
                                        highestTxId,
                                        takeSessionId,
                                        versionedRing,
                                        ringMember,
                                        takerIsOnline,
                                        availableStream);
                                } else {
                                    LOG.warn("Highest txId unavailable for {}", versionedPartitionName);
                                    return Long.MAX_VALUE;
                                }
                            });
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
        } else {
            return Long.MAX_VALUE;
        }
    }

    private boolean takerIsOnline(RingMember ringMember, long takeSessionId, VersionedAquarium versionedAquarium) throws Exception {
        Long onlineSessionId = onlineTakers.get(ringMember);
        if (onlineSessionId != null && onlineSessionId == takeSessionId) {
            return true;
        }
        boolean online = versionedAquarium.isLivelyEndState(ringMember);
        if (online) {
            onlineTakers.put(ringMember, takeSessionId);
        }
        return online;
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

            AtomicBoolean available = new AtomicBoolean(false);
            long reofferDelta = (isSystemPartition ? systemReofferDeltaMillis : reofferDeltaMillis);
            long reofferAfterTimeInMillis = System.currentTimeMillis() + reofferDelta;

            took.compute(ringMember, (RingMember t, SessionedTxId u) -> {
                try {
                    if (u == null) {
                        available.set(true);
                        u = new SessionedTxId(takeSessionId, highestTxId, reofferAfterTimeInMillis, -1, false);
                    } else if (u.sessionId != takeSessionId) {
                        available.set(true);
                        u = new SessionedTxId(takeSessionId, highestTxId, reofferAfterTimeInMillis, -1, false);
                    } else if (isSystemPartition && !u.tookFully) {
                        available.set(true);
                        u = new SessionedTxId(takeSessionId, highestTxId, reofferAfterTimeInMillis, u.tookTxId, false);
                    } else if (isSufficientCategory && (!takerIsOnline || shouldOffer(u, highestTxId))) {
                        available.set(true);
                        u = new SessionedTxId(takeSessionId, highestTxId, reofferAfterTimeInMillis, u.tookTxId, u.tookFully);
                    }
                    return u;
                } catch (Exception x) {
                    throw new RuntimeException(x);
                }
            });
            if (available.get()) {
                steadyState.remove(ringMember);
                availableStream.available(versionedPartitionName, highestTxId);
                return reofferDelta;
            } else {
                steadyState.put(ringMember, takeSessionId);
                return Long.MAX_VALUE;
            }
        }
        if (category == null) {
            return Long.MAX_VALUE;
        }
        return category * slowTakeMillis;
    }

    private boolean shouldOffer(SessionedTxId u, long highestTxId) {
        return highestTxId > -1 && (highestTxId > u.offeredTxId || (highestTxId > u.tookTxId && System.currentTimeMillis() > u.reofferAtTimeInMillis));
    }

    void updateTxId(VersionedRing versionedRing, int takeFromFactor, long updateTxId, boolean invalidateOnline) throws Exception {
        if (expunged.get()) {
            return;
        }

        updateCategory(versionedRing, takeFromFactor, updateTxId);

        synchronized (steadyState) {
            steadyState.clear();
            if (invalidateOnline) {
                onlineTakers.clear();
            }
        }
    }

    void rowsTaken(TxHighestPartitionTx<Long> txHighestPartitionTx,
        long takeSessionId,
        VersionedAquarium versionedAquarium,
        VersionedRing versionedRing,
        RingMember remoteRingMember,
        long localTxId,
        int takeFromFactor) throws Exception {

        if (expunged.get()) {
            return;
        }

        took.compute(remoteRingMember, (key, existingSessionedTxId) -> {
            if (existingSessionedTxId != null) {
                long tookTxId = Math.max(localTxId, existingSessionedTxId.tookTxId);
                boolean tookFully = (tookTxId >= existingSessionedTxId.offeredTxId);
                return new SessionedTxId(existingSessionedTxId.sessionId,
                    existingSessionedTxId.offeredTxId,
                    existingSessionedTxId.reofferAtTimeInMillis,
                    tookTxId,
                    tookFully);
            }
            return null;
        });
        synchronized (steadyState) {
            txHighestPartitionTx.tx(versionedAquarium, (versionedAquarium1, highestTxId) -> {
                if (localTxId >= highestTxId) {
                    steadyState.put(remoteRingMember, takeSessionId);
                }
                return null;
            });
        }
        updateCategory(versionedRing, takeFromFactor, localTxId);
    }

    //TODO call this?
    void cleanup(Set<RingMember> retain) {
        if (expunged.get()) {
            return;
        }

        ConcurrentHashMap.KeySetView<RingMember, SessionedTxId> keySet = took.keySet();
        keySet.removeAll(Sets.difference(keySet, retain));
    }

    private void updateCategory(VersionedRing versionedRing, int takeFromFactor, long latestTxId) throws Exception {
        if (takeFromFactor > 0) {
            long currentTimeTxId = timestampedOrderIdProvider.getApproximateId(System.currentTimeMillis());
            int fastEnough = 0;
            int worstCategory = 1;
            for (Entry<RingMember, Integer> candidate : versionedRing.members.entrySet()) {
                if (fastEnough < Math.max(versionedRing.takeFromFactor, takeFromFactor)) {
                    SessionedTxId lastTxId = took.get(candidate.getKey());
                    if (lastTxId != null) {
                        if (lastTxId.tookTxId == latestTxId) {
                            fastEnough++;
                        } else {
                            long latency = currentTimeTxId - lastTxId.offeredTxId;
                            if (latency < slowTakeId * candidate.getValue()) {
                                worstCategory = Math.max(worstCategory, candidate.getValue());
                                fastEnough++;
                            }
                        }
                    }
                } else if (candidate.getValue() > worstCategory) {
                    took.remove(candidate.getKey());
                }
            }
            currentCategory.set(worstCategory);
        }

    }

    public void expunged() {
        expunged.set(true);
        took.clear();
    }

    boolean streamTookLatencies(VersionedRing versionedRing, TookLatencyStream stream) throws Exception {
        long currentTimeTxId = timestampedOrderIdProvider.getApproximateId(System.currentTimeMillis());
        for (Entry<RingMember, Integer> candidate : versionedRing.members.entrySet()) {
            RingMember ringMember = candidate.getKey();
            int category = candidate.getValue();
            SessionedTxId lastTxId = took.get(ringMember);
            if (lastTxId != null) {
                long latency = currentTimeTxId - lastTxId.offeredTxId;
                long tooSlowLatency = slowTakeId * category;
                if (!stream.stream(ringMember, latency, category, tooSlowLatency - latency)) {
                    return false;
                }
            } else if (!stream.stream(ringMember, -1L, category, -1L)) {
                return false;
            }
        }
        return true;
    }

    static class SessionedTxId {

        final long sessionId;
        final long offeredTxId;
        final long reofferAtTimeInMillis;
        final long tookTxId;
        final boolean tookFully;

        public SessionedTxId(long sessionId, long offeredTxId, long reofferAtTimeInMillis, long tookTxId, boolean tookFully) {
            this.sessionId = sessionId;
            this.offeredTxId = offeredTxId;
            this.reofferAtTimeInMillis = reofferAtTimeInMillis;
            this.tookTxId = tookTxId;
            this.tookFully = tookFully;
        }
    }
}
