package com.jivesoftware.os.amza.shared.take;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.shared.partition.TxHighestPartitionTx;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.amza.shared.take.TakeRingCoordinator.VersionedRing;
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
    final VersionedPartitionName versionedPartitionName;
    final TimestampedOrderIdProvider timestampedOrderIdProvider;
    final long slowTakeMillis;
    final long slowTakeId;
    final long systemReofferDeltaMillis;
    final long reofferDeltaMillis;
    final AtomicInteger currentCategory;

    final ConcurrentHashMap<RingMember, SessionedTxId> took = new ConcurrentHashMap<>();
    final ConcurrentHashMap<RingMember, Long> steadyState = new ConcurrentHashMap<>();

    public TakeVersionedPartitionCoordinator(VersionedPartitionName versionedPartitionName,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        long slowTakeMillis,
        long slowTakeId,
        long systemReofferDeltaMillis,
        long reofferDeltaMillis) {

        this.versionedPartitionName = versionedPartitionName;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.systemReofferDeltaMillis = systemReofferDeltaMillis;
        this.reofferDeltaMillis = reofferDeltaMillis;
        this.slowTakeMillis = slowTakeMillis;
        this.slowTakeId = slowTakeId;
        this.currentCategory = new AtomicInteger(1);
    }

    boolean isSteadyState(RingMember ringMember, long takeSessionId) {
        Long steadySessionId = steadyState.get(ringMember);
        return steadySessionId != null && steadySessionId == takeSessionId;
    }

    Long availableRowsStream(TxHighestPartitionTx<Long> txHighestPartitionTx,
        long takeSessionId,
        VersionedRing versionedRing,
        RingMember ringMember,
        boolean takerIsOnline,
        int takeFromFactor,
        AvailableStream availableStream) throws Exception {

        if (takeFromFactor > 0) {
            synchronized (steadyState) {
                return txHighestPartitionTx.tx(versionedPartitionName.getPartitionName(),
                    (versionedPartitionName1, livelyEndState, highestTxId) -> {
                        if (livelyEndState == null) {
                            // no storage, likely not a member of the ring
                            return Long.MAX_VALUE;
                        }
                        /*if (versionedPartitionName1 == null || versionedPartitionName1.getPartitionVersion() != versionedPartitionName.getPartitionVersion
                        ()) {
                            return Long.MAX_VALUE;
                        }*/

                        Integer category = versionedRing.getCategory(ringMember);
                        if (!takerIsOnline
                            || livelyEndState.getCurrentState() == State.bootstrap
                            || (category != null && category <= currentCategory.get())) {

                            AtomicBoolean available = new AtomicBoolean(false);
                            long reofferDelta = ((versionedPartitionName.getPartitionName().isSystemPartition()) ? systemReofferDeltaMillis :
                                reofferDeltaMillis);
                            long reofferAfterTimeInMillis = System.currentTimeMillis() + reofferDelta;

                            took.compute(ringMember, (RingMember t, SessionedTxId u) -> {
                                try {
                                    if (u == null) {
                                        /*LOG.info("NEW (MISSING): candidateCategory:{} currentCategory:{} ringMember:{} " +
                                         "nudged:{} state:{} txId:{} takerIsOnline:{}",
                                         category, currentCategory.get(), ringMember, versionedPartitionName, partitionWaterlineState, highestTxId,
                                         takerIsOnline);*/
                                        available.set(true);
                                        return new SessionedTxId(takeSessionId, highestTxId, reofferAfterTimeInMillis, -1);
                                    } else {
                                        if (u.sessionId != takeSessionId) {
                                            /*LOG.info("NEW (SESSION): oldSession:{} newSession:{} " +
                                             "candidateCategory:{} currentCategory:{} ringMember:{} nudged:{} state:{} txId:{}",
                                             u.sessionId, takeSessionId,
                                             category, currentCategory.get(), ringMember, versionedPartitionName, partitionWaterlineState, highestTxId);*/
                                            available.set(true);
                                            return new SessionedTxId(takeSessionId, highestTxId, reofferAfterTimeInMillis, -1);
                                        } else {
                                            if (highestTxId > -1 &&
                                                (highestTxId > u.offeredTxId ||
                                                    (highestTxId > u.tookTxId && System.currentTimeMillis() > u.reofferAtTimeInMillis))) {
                                                /*LOG.info("NEW (TX): candidateCategory:{} currentCategory:{} ringMember:{} " +
                                                 "nudged:{} state:{} tookTxId:{} txId:{}",
                                                 category, currentCategory.get(), ringMember, versionedPartitionName, partitionWaterlineState, u.tookTxId,
                                                 highestTxId);*/
                                                available.set(true);
                                                return new SessionedTxId(takeSessionId, highestTxId, reofferAfterTimeInMillis, u.tookTxId);
                                            } else if (!takerIsOnline) {
                                                /*LOG.info("NEW (OFFLINE): candidateCategory:{} currentCategory:{} ringMember:{} " +
                                                  "nudged:{} state:{} tookTxId:{} txId:{}",
                                                  category, currentCategory.get(), ringMember, versionedPartitionName, livelyEndState, u.tookTxId,
                                                  highestTxId);*/
                                                available.set(true);
                                                return u;
                                            } else {
                                                return u;
                                            }
                                        }
                                    }
                                } catch (Exception x) {
                                    throw new RuntimeException(x);
                                }
                            });
                            if (available.get()) {
                                steadyState.remove(ringMember);
                                availableStream.available(versionedPartitionName, highestTxId);
                                return reofferDelta;
                            } else {
                                //LOG.info("Found steady state for member:{} partition:{} session:{}", ringMember, versionedPartitionName, takeSessionId);
                                steadyState.put(ringMember, takeSessionId);
                                return Long.MAX_VALUE;
                            }
                        }
                        if (category == null) {
                            return Long.MAX_VALUE;
                        }
                        return category * slowTakeMillis;
                    });
            }

        } else {
            return Long.MAX_VALUE;
        }
    }

    void updateTxId(VersionedRing versionedRing, int takeFromFactor, long updateTxId) throws Exception {
        updateCategory(versionedRing, takeFromFactor, updateTxId);

        synchronized (steadyState) {
            steadyState.clear();
        }

        //LOG.info("UPDATE: partition:{} state:{} txId:{} ", versionedPartitionName, state, txId);
    }

    void rowsTaken(TxHighestPartitionTx<Long> txHighestPartitionTx,
        long takeSessionId,
        VersionedRing versionedRing,
        RingMember remoteRingMember,
        long localTxId,
        int takeFromFactor) throws Exception {
        took.compute(remoteRingMember, (key, existingSessionedTxId) -> {
            if (existingSessionedTxId != null) {
                return new SessionedTxId(existingSessionedTxId.sessionId,
                    existingSessionedTxId.offeredTxId,
                    existingSessionedTxId.reofferAtTimeInMillis,
                    Math.max(localTxId, existingSessionedTxId.tookTxId));
            } else {
                //LOG.info("NO SESSION: remote:{} partition:{} state:{} txId:{}",
                //    remoteRingMember, versionedPartitionName, state.get(), localTxId);
            }
            return null;
        });
        synchronized (steadyState) {
            txHighestPartitionTx.tx(versionedPartitionName.getPartitionName(), (versionedPartitionName1, livelyEndState, highestTxId) -> {
                if (localTxId >= highestTxId) {
                    //LOG.info("Took to steady state for member:{} partition:{} session:{}", remoteRingMember, versionedPartitionName, takeSessionId);
                    steadyState.put(remoteRingMember, takeSessionId);
                }
                return null;
            });
        }
        updateCategory(versionedRing, takeFromFactor, localTxId);
    }

    //TODO call this?
    void cleanup(Set<RingMember> retain) {
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
                    if (took.remove(candidate.getKey()) != null) {
                        //LOG.info("REMOVED SESSION: candidateCategory:{} worstCategory:{} partition:{} state:{} txId:{}",
                        //    candidate.getValue(), worstCategory, versionedPartitionName, state.get(), latestTxId);
                    }
                }
            }
            currentCategory.set(worstCategory);
        }

    }

    static class SessionedTxId {

        final long sessionId;
        final long offeredTxId;
        final long reofferAtTimeInMillis;
        final long tookTxId;

        public SessionedTxId(long sessionId, long offeredTxId, long reofferAtTimeInMillis, long tookTxId) {
            this.sessionId = sessionId;
            this.offeredTxId = offeredTxId;
            this.reofferAtTimeInMillis = reofferAtTimeInMillis;
            this.tookTxId = tookTxId;
        }
    }
}
