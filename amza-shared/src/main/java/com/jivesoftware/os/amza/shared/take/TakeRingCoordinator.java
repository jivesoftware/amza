package com.jivesoftware.os.amza.shared.take;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker.PartitionUpdatedStream;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.util.LinkedHashMap;
import java.util.Map;
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

    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final AtomicReference<VersionedRing> versionedRing = new AtomicReference<>();
    private final ConcurrentHashMap<PartitionName, TakePartitionCoordinator> partitionCoordinators = new ConcurrentHashMap<>();

    public TakeRingCoordinator(TimestampedOrderIdProvider timestampedOrderIdProvider, Map.Entry<RingMember, RingHost>[] ringMembers) {
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.versionedRing.compareAndSet(null, new VersionedRing(timestampedOrderIdProvider.nextId(), ringMembers));
    }

    void cya(Set<PartitionName> retain) {
        ConcurrentHashMap.KeySetView<PartitionName, TakePartitionCoordinator> keySet = partitionCoordinators.keySet();
        keySet.removeAll(Sets.difference(keySet, retain));
    }

    void take(RingMember ringMember, long takeSessionId, PartitionUpdatedStream updatedPartitionsStream) throws Exception {
        for (TakePartitionCoordinator coordinator : partitionCoordinators.values()) {
            coordinator.take(takeSessionId, versionedRing.get(), ringMember, timestampedOrderIdProvider, updatedPartitionsStream);
        }
    }

    void update(Entry<RingMember, RingHost>[] aboveRing, VersionedPartitionName versionedPartitionName, long txId) {
        VersionedRing ring = versionedRing.updateAndGet((existing) -> {
            return existing.isStillValid(aboveRing) ? existing : new VersionedRing(timestampedOrderIdProvider.nextId(), aboveRing);
        });
        long slowTakeInMillis = 60_000L; // TODO config
        TakePartitionCoordinator coordinator = partitionCoordinators.computeIfAbsent(versionedPartitionName.getPartitionName(), (key) -> {
            return new TakePartitionCoordinator(versionedPartitionName.getPartitionName(),
                new AtomicLong(txId), timestampedOrderIdProvider.getApproximateId(slowTakeInMillis));
        });
        coordinator.updateTxId(ring, txId);
    }

    void took(RingMember ringMember, PartitionName partitionName, long txId) {
        TakePartitionCoordinator coordinator = partitionCoordinators.get(partitionName);
        if (coordinator != null) {
            coordinator.took(versionedRing.get(), ringMember, txId);
        }
    }

    static public class VersionedRing {

        final long version;
        final int takeFromFactor;
        final LinkedHashMap<RingMember, Integer> members;

        public VersionedRing(long version, Map.Entry<RingMember, RingHost>[] ringMembers) {
            this.version = version;
            members = new LinkedHashMap<>();
            takeFromFactor = 1 + (int) Math.sqrt(ringMembers.length);
            int taken = takeFromFactor;
            int category = 1;
            for (int start = 0; start < ringMembers.length; start++) {
                if (ringMembers[start] == null) {
                    continue;
                }
                for (int offset = 1, loops = 0; offset < ringMembers.length; loops++, offset = (int) Math.pow(2, loops)) {
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

        boolean isStillValid(Map.Entry<RingMember, RingHost>[] aboveRing) {
            if (aboveRing.length != members.size()) {
                return false;
            }
            int i = 0;
            for (RingMember ringMember : members.keySet()) {
                if (ringMember.equals(aboveRing[i].getKey())) {
                    i++;
                } else {
                    return false;
                }
            }
            return true;
        }
    }

}
