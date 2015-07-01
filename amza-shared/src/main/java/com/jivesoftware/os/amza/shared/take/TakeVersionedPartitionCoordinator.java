package com.jivesoftware.os.amza.shared.take;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.RowsTaker.AvailableStream;
import com.jivesoftware.os.amza.shared.take.TakeRingCoordinator.VersionedRing;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class TakeVersionedPartitionCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    final PartitionProperties properties;
    final VersionedPartitionName versionedPartitionName;
    final AtomicReference<Status> status;
    final AtomicLong txId;
    final long slowTakeMillis;
    final long slowTakeId;
    final ConcurrentHashMap<RingMember, SessionedTxId> took = new ConcurrentHashMap<>();
    final AtomicInteger currentCategory;

    public TakeVersionedPartitionCoordinator(PartitionProperties properties,
        VersionedPartitionName versionedPartitionName,
        Status status,
        AtomicLong txId,
        long slowTakeMillis,
        long slowTakeId) {

        this.properties = properties;
        this.versionedPartitionName = versionedPartitionName;
        this.status = new AtomicReference<>(status);
        this.txId = txId;
        this.slowTakeMillis = slowTakeMillis;
        this.slowTakeId = slowTakeId;
        this.currentCategory = new AtomicInteger(properties.takeFromFactor > 0 ? 1 : -1);
    }

    void updateTxId(VersionedRing versionedRing, Status status, long txId) {
        this.status.set(status);
        if (this.txId.get() < txId) {
            this.txId.set(txId);
            updateCategory(versionedRing);
            //LOG.info("UPDATE: partition:{} status:{} txId:{} ", versionedPartitionName, status, txId);
        }
    }

    long availableRowsStream(long takeSessionId,
        VersionedRing versionedRing,
        RingMember ringMember,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        AvailableStream availableStream) throws Exception {

        long takeTxId = txId.get();
        Status currentStatus = status.get();
        if (currentStatus == Status.ONLINE && currentCategory.get() > -1) {

            Integer category = versionedRing.getCategory(ringMember);
            if (category != null && category <= currentCategory.get()) {
                took.compute(ringMember, (RingMember t, SessionedTxId u) -> {
                    try {
                        if (u == null) {
                            availableStream.available(versionedPartitionName, currentStatus, takeTxId);
                            return new SessionedTxId(takeSessionId, takeTxId - slowTakeId);
                        } else {
                            if (u.sessionId != takeSessionId) {
                                availableStream.available(versionedPartitionName, currentStatus, takeTxId);
                                //LOG.info("NEW: ringMember:{} nudged:{} status:{} txId:{}", ringMember, versionedPartitionName, currentStatus, takeTxId);
                                return new SessionedTxId(takeSessionId, takeTxId);
                            } else {
                                if (u.txId < takeTxId) {
                                    availableStream.available(versionedPartitionName, currentStatus, takeTxId);
                                    return new SessionedTxId(takeSessionId, takeTxId);
                                } else {
                                    return u;
                                }
                            }
                        }
                    } catch (Exception x) {
                        throw new RuntimeException(x);
                    }
                });
                return Long.MAX_VALUE;
            }
            if (category == null) {
                return Long.MAX_VALUE;
            }
            return category * slowTakeMillis;
        } else {
            if (properties.takeFromFactor > 0) {
                availableStream.available(versionedPartitionName, currentStatus, takeTxId);
            }
            return Long.MAX_VALUE;
        }
    }

    void rowsTaken(VersionedRing versionedRing, RingMember remoteRingMember, long localTxId) {
        if (status.get() == Status.ONLINE) {
            took.compute(remoteRingMember, (key, existingSessionedTxId) -> {
                if (existingSessionedTxId != null) {
                    return new SessionedTxId(existingSessionedTxId.sessionId, localTxId);
                }
                return null;
            });
        }
        updateCategory(versionedRing);

    }

    //TODO call this?
    void cleanup(Set<RingMember> retain) {
        ConcurrentHashMap.KeySetView<RingMember, SessionedTxId> keySet = took.keySet();
        keySet.removeAll(Sets.difference(keySet, retain));
    }

    private void updateCategory(VersionedRing versionedRing) {

        if (properties.takeFromFactor == 0) {
            currentCategory.set(-1);
        } else {
            long currentTxId = txId.get();
            int fastEnough = 0;
            int worstCategory = 1;
            for (Entry<RingMember, Integer> candidate : versionedRing.members.entrySet()) {
                if (fastEnough < Math.max(versionedRing.takeFromFactor, properties.takeFromFactor)) {
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
