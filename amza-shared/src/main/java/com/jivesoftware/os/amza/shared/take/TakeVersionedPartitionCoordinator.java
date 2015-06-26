package com.jivesoftware.os.amza.shared.take;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.TakeRingCoordinator.VersionedRing;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker.PartitionUpdatedStream;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class TakeVersionedPartitionCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    final VersionedPartitionName versionedPartitionName;
    final AtomicReference<Status> status = new AtomicReference<>();
    final AtomicLong txId;
    final long slowTakeMillis;
    final long slowTakeId;
    final ConcurrentHashMap<RingMember, SessionedTxId> took = new ConcurrentHashMap<>();
    final AtomicReference<Integer> currentCategory = new AtomicReference<>();
    final AtomicLong lastTakeSessionId = new AtomicLong(0);

    public TakeVersionedPartitionCoordinator(VersionedPartitionName versionedPartitionName, AtomicLong txId, long slowTakeMillis, long slowTakeId) {
        this.versionedPartitionName = versionedPartitionName;
        this.txId = txId;
        this.slowTakeMillis = slowTakeMillis;
        this.slowTakeId = slowTakeId;
    }

    void updateTxId(VersionedRing versionedRing, Status status, long txId) {
        this.status.set(status);
        if (this.txId.get() < txId) {
            this.txId.set(txId);
            updateCategory(versionedRing);
        }
    }

    long take(long takeSessionId,
        VersionedRing versionedRing,
        RingMember ringMember,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        PartitionUpdatedStream updatedPartitionsStream) throws Exception {

        Integer category = versionedRing.getCategory(ringMember);
        if (category != null && category <= currentCategory.get()) {
            long takeTxId = txId.get();
            took.compute(ringMember, (RingMember t, SessionedTxId u) -> {
                if (u != null) {
                    try {
                        if (u.sessionId != takeSessionId) {
                            updatedPartitionsStream.update(versionedPartitionName, status.get(), takeTxId);// TODO add PartitionStatus, txId
                            return new SessionedTxId(takeSessionId, takeTxId);
                        } else {
                            if (u.txId < takeTxId) {
                                updatedPartitionsStream.update(versionedPartitionName, status.get(), takeTxId);// TODO add PartitionStatus, txId
                            }
                            return u;
                        }
                    } catch (Exception x) {
                        throw new RuntimeException(x);
                    }
                } else {
                    return new SessionedTxId(takeSessionId, takeTxId - slowTakeId);
                }
            });
            return Long.MAX_VALUE;
        }
        if (category == null) {
            return Long.MAX_VALUE;
        }
        return category * slowTakeMillis;
    }

    void took(VersionedRing versionedRing, RingMember ringMember, long txId) {
        took.compute(ringMember, (RingMember t, SessionedTxId u) -> {
            if (u != null) {
                return new SessionedTxId(u.sessionId, txId);
            }
            return null;
        });
        updateCategory(versionedRing);
    }

    void cleanup(Set<RingMember> retain) {
        ConcurrentHashMap.KeySetView<RingMember, SessionedTxId> keySet = took.keySet();
        keySet.removeAll(Sets.difference(keySet, retain));
    }

    private void updateCategory(VersionedRing versionedRing) {

        long currentTxId = txId.get();
        int fastEnough = 0;
        int worstCategory = 0;
        for (Entry<RingMember, Integer> candidate : versionedRing.members.entrySet()) {
            if (fastEnough < versionedRing.takeFromFactor) {
                SessionedTxId lastTxId = took.get(candidate.getKey());
                if (lastTxId != null) {
                    long latency = currentTxId - lastTxId.txId;
                    if (latency < slowTakeId * candidate.getValue()) {
                        worstCategory = Math.max(worstCategory, candidate.getValue());
                        fastEnough++;
                    }
                }
            } else if (candidate.getValue() > worstCategory) {
                took.remove(candidate.getKey());
            }
        }
        currentCategory.set(worstCategory);

    }

    static class SessionedTxId {

        final long sessionId;
        final long txId;

        public SessionedTxId(long sessionId, long txId) {
            this.sessionId = sessionId;
            this.txId = txId;
        }

    }
}
