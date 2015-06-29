package com.jivesoftware.os.amza.shared.take;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.RowsTaker.AvailableStream;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class TakeRingCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String ringName;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final long slowTakeInMillis = 60_000L; // TODO config
    private final AtomicReference<VersionedRing> versionedRing = new AtomicReference<>();
    private final ConcurrentHashMap<VersionedPartitionName, TakeVersionedPartitionCoordinator> partitionCoordinators = new ConcurrentHashMap<>();

    public TakeRingCoordinator(String ringName, TimestampedOrderIdProvider timestampedOrderIdProvider, List<Entry<RingMember, RingHost>> neighbors) {
        this.ringName = ringName;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        LOG.info("INITIALIZED RING:" + ringName + " size:" + neighbors.size());
        this.versionedRing.compareAndSet(null, new VersionedRing(timestampedOrderIdProvider.nextId(), neighbors));
    }

    void cya(List<Entry<RingMember, RingHost>> neighbors, Set<VersionedPartitionName> retain) {
        ConcurrentHashMap.KeySetView<VersionedPartitionName, TakeVersionedPartitionCoordinator> keySet = partitionCoordinators.keySet();
        keySet.removeAll(Sets.difference(keySet, retain));
        for (VersionedPartitionName add : Sets.difference(retain, keySet)) {
            ensureCoordinator(add, 0, slowTakeInMillis);
        }
        ensureVersionedRing(neighbors);
    }

    void update(List<Entry<RingMember, RingHost>> neighbors, VersionedPartitionName versionedPartitionName, Status status, long txId) {
        VersionedRing ring = ensureVersionedRing(neighbors);
        TakeVersionedPartitionCoordinator coordinator = ensureCoordinator(versionedPartitionName, txId, slowTakeInMillis);
        coordinator.updateTxId(ring, status, txId);
    }

    long availableRowsStream(RingMember ringMember, long takeSessionId, AvailableStream availableStream) throws Exception {
        long suggestedWaitInMillis = Long.MAX_VALUE;
        VersionedRing ring = versionedRing.get();
        for (TakeVersionedPartitionCoordinator coordinator : partitionCoordinators.values()) {
            suggestedWaitInMillis = Math.min(suggestedWaitInMillis,
                coordinator.availableRowsStream(takeSessionId, ring, ringMember, timestampedOrderIdProvider, availableStream)
            );
        }
        return suggestedWaitInMillis;
    }

    void rowsTaken(RingMember remoteRingMember, VersionedPartitionName localVersionedPartitionName, long localTxId) {
        TakeVersionedPartitionCoordinator coordinator = partitionCoordinators.get(localVersionedPartitionName);
        if (coordinator != null) {
            coordinator.rowsTaken(versionedRing.get(), remoteRingMember, localTxId);
        }
    }

    private TakeVersionedPartitionCoordinator ensureCoordinator(VersionedPartitionName versionedPartitionName, long txId, long slowTakeInMillis) {
        return partitionCoordinators.computeIfAbsent(versionedPartitionName, (key) -> {
            return new TakeVersionedPartitionCoordinator(versionedPartitionName,
                new AtomicLong(txId), slowTakeInMillis, timestampedOrderIdProvider.getApproximateId(slowTakeInMillis));
        });
    }

    private VersionedRing ensureVersionedRing(List<Entry<RingMember, RingHost>> neighbors) {
        return versionedRing.updateAndGet((existing) -> {
            if (existing.isStillValid(neighbors)) {
                return existing;
            } else {
                LOG.info("RESIZED RING:" + ringName + " size:" + neighbors.size() + " " + this + " " + existing);
                return new VersionedRing(timestampedOrderIdProvider.nextId(), neighbors);
            }
        });
    }

    static public class VersionedRing {

        final long version;
        final int takeFromFactor;
        final LinkedHashMap<RingMember, Integer> members;

        public VersionedRing(long version, List<Entry<RingMember, RingHost>> neighbors) {
            this.version = version;

            Entry<RingMember, RingHost>[] ringMembers = neighbors.toArray(new Entry[neighbors.size()]);
            members = new LinkedHashMap<>();
            takeFromFactor = 1 + (int) Math.sqrt(ringMembers.length);
            int taken = takeFromFactor;
            int category = 1;
            for (int start = 0; start < ringMembers.length; start++) {
                if (ringMembers[start] == null) {
                    continue;
                }
                for (int offset = 1, loops = 0; offset <= ringMembers.length; loops++, offset = (int) Math.pow(2, loops)) {
                    int memberIndex = (start + (offset - 1)) % ringMembers.length;
                    if (ringMembers[memberIndex] == null) {
                        continue;
                    }
                    members.put(ringMembers[memberIndex].getKey(), category);
                    ringMembers[memberIndex] = null;

                    taken--;
                    if (taken == 0) {
                        taken = takeFromFactor;
                        category++;
                    }
                }
            }
        }

        public Integer getCategory(RingMember ringMember) {
            return members.get(ringMember);
        }

        boolean isStillValid(List<Entry<RingMember, RingHost>> neighbors) {
            if (neighbors.size() != members.size()) {
                return false;
            }
            for (Entry<RingMember, RingHost> neighbor : neighbors) {
                if (!members.containsKey(neighbor.getKey())) {
                    return false;
                }
            }
            return true;
        }
    }

}
