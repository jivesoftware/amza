package com.jivesoftware.os.amza.shared.take;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.amza.shared.take.TakeRingCoordinator.VersionedRing;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class TakeVersionedPartitionCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    final VersionedPartitionName versionedPartitionName;
    final TimestampedOrderIdProvider timestampedOrderIdProvider;
    final AtomicReference<Status> status;
    final AtomicLong txId;
    final long slowTakeMillis;
    final long slowTakeId;
    final long systemReofferDeltaMillis;
    final long reofferDeltaMillis;
    final ConcurrentHashMap<RingMember, SessionedTxId> took = new ConcurrentHashMap<>();
    final AtomicInteger currentCategory;

    public TakeVersionedPartitionCoordinator(VersionedPartitionName versionedPartitionName,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        Status status,
        AtomicLong txId,
        long slowTakeMillis,
        long slowTakeId,
        long systemReofferDeltaMillis,
        long reofferDeltaMillis) {

        this.versionedPartitionName = versionedPartitionName;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.systemReofferDeltaMillis = systemReofferDeltaMillis;
        this.reofferDeltaMillis = reofferDeltaMillis;
        this.status = new AtomicReference<>(status);
        this.txId = txId;
        this.slowTakeMillis = slowTakeMillis;
        this.slowTakeId = slowTakeId;
        this.currentCategory = new AtomicInteger(1);
    }

    void updateTxId(VersionedRing versionedRing, Status status, long txId, int takeFromFactor) {
        this.status.set(status);
        if (this.txId.get() < txId) {
            this.txId.set(txId);
            updateCategory(versionedRing, takeFromFactor);
            //LOG.info("UPDATE: partition:{} status:{} txId:{} ", versionedPartitionName, status, txId);
        }
    }

    long availableRowsStream(long takeSessionId,
        VersionedRing versionedRing,
        RingMember ringMember,
        int takeFromFactor,
        AvailableStream availableStream) throws Exception {

        long offerTxId = txId.get();
        Status currentStatus = status.get();
        if (takeFromFactor > 0) {

            Integer category = versionedRing.getCategory(ringMember);
            if (status.get() == Status.KETCHUP || (category != null && category <= currentCategory.get())) {
                AtomicBoolean available = new AtomicBoolean(false);
                long reofferDelta = ((versionedPartitionName.getPartitionName().isSystemPartition()) ? systemReofferDeltaMillis : reofferDeltaMillis);
                long reofferAfterTimeInMillis = System.currentTimeMillis() + reofferDelta;

                took.compute(ringMember, (RingMember t, SessionedTxId u) -> {
                    try {
                        if (u == null) {
                            //LOG.info("NEW (MISSING): candidateCategory:{} currentCategory:{} ringMember:{} nudged:{} status:{} txId:{}",
                            //    category, currentCategory.get(), ringMember, versionedPartitionName, currentStatus, offerTxId);
                            available.set(true);
                            return new SessionedTxId(takeSessionId, offerTxId, reofferAfterTimeInMillis, -1);
                        } else {
                            if (u.sessionId != takeSessionId) {
                                /*
                                 LOG.info(
                                 "NEW (SESSION): oldSession:{} newSession:{} " +
                                 "candidateCategory:{} currentCategory:{} ringMember:{} nudged:{} status:{} txId:{}",
                                 u.sessionId, takeSessionId,
                                 category, currentCategory.get(), ringMember, versionedPartitionName, currentStatus, offerTxId);
                                 */
                                available.set(true);
                                return new SessionedTxId(takeSessionId, offerTxId, reofferAfterTimeInMillis, -1);
                            } else {
                                if (offerTxId > u.offeredTxId || (offerTxId > u.tookTxId && System.currentTimeMillis() > u.reofferAtTimeInMillis)) {
                                    //LOG.info("NEW (TX): candidateCategory:{} currentCategory:{} ringMember:{} nudged:{} status:{} tookTxId:{} offerTxId:{}",
                                    //    category, currentCategory.get(), ringMember, versionedPartitionName, currentStatus, u.tookTxId, offerTxId);
                                    available.set(true);
                                    return new SessionedTxId(takeSessionId, offerTxId, reofferAfterTimeInMillis, u.tookTxId);
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
                    availableStream.available(versionedPartitionName, currentStatus, offerTxId);
                    return reofferDelta;
                } else {
                    return Long.MAX_VALUE;
                }
            }
            if (category == null) {
                return Long.MAX_VALUE;
            }
            return category * slowTakeMillis;
        } else {
            return Long.MAX_VALUE;
        }
    }

    void rowsTaken(VersionedRing versionedRing, RingMember remoteRingMember, long localTxId, int takeFromFactor) {
        if (status.get() == Status.ONLINE) {
            took.compute(remoteRingMember, (key, existingSessionedTxId) -> {
                if (existingSessionedTxId != null) {
                    return new SessionedTxId(existingSessionedTxId.sessionId,
                        existingSessionedTxId.offeredTxId,
                        existingSessionedTxId.reofferAtTimeInMillis,
                        Math.max(localTxId, existingSessionedTxId.tookTxId));
                } else {
                    //LOG.info("NO SESSION: remote:{} partition:{} status:{} txId:{}",
                    //    remoteRingMember, versionedPartitionName, status.get(), localTxId);
                }
                return null;
            });
        } else {
            //LOG.info("NOT ONLINE: remote:{} partition:{} status:{} txId:{}",
            //    remoteRingMember, versionedPartitionName, status.get(), localTxId);
        }
        updateCategory(versionedRing, takeFromFactor);

    }

    //TODO call this?
    void cleanup(Set<RingMember> retain) {
        ConcurrentHashMap.KeySetView<RingMember, SessionedTxId> keySet = took.keySet();
        keySet.removeAll(Sets.difference(keySet, retain));
    }

    private void updateCategory(VersionedRing versionedRing, int takeFromFactor) {

        if (status.get() == Status.ONLINE && takeFromFactor > 0) {
            long currentTimeTxId = timestampedOrderIdProvider.getApproximateId(System.currentTimeMillis());
            long latestTxId = txId.get();
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
                        //LOG.info("REMOVED SESSION: candidateCategory:{} worstCategory:{} partition:{} status:{} txId:{}",
                        //    candidate.getValue(), worstCategory, versionedPartitionName, status.get(), latestTxId);
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
