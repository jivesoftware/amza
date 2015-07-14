package com.jivesoftware.os.amza.shared.take;

import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
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
 * @author jonathan.colt
 */
public class TakeRingCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final byte[] ringName;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final IdPacker idPacker;
    private final VersionedPartitionProvider versionedPartitionProvider;
    private final long slowTakeInMillis;
    private final long systemReofferDeltaMillis;
    private final long reofferDeltaMillis;
    private final AtomicReference<VersionedRing> versionedRing = new AtomicReference<>();
    private final ConcurrentHashMap<VersionedPartitionName, TakeVersionedPartitionCoordinator> partitionCoordinators = new ConcurrentHashMap<>();

    public TakeRingCoordinator(byte[] ringName,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        IdPacker idPacker,
        VersionedPartitionProvider versionedPartitionProvider,
        long systemReofferDeltaMillis,
        long slowTakeInMillis,
        long reofferDeltaMillis, List<Entry<RingMember, RingHost>> neighbors) {
        this.ringName = ringName;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.idPacker = idPacker;
        this.versionedPartitionProvider = versionedPartitionProvider;
        this.slowTakeInMillis = slowTakeInMillis;
        this.systemReofferDeltaMillis = systemReofferDeltaMillis;
        this.reofferDeltaMillis = reofferDeltaMillis;
        //LOG.info("INITIALIZED RING:" + ringName + " size:" + neighbors.size());
        this.versionedRing.compareAndSet(null, new VersionedRing(neighbors));
    }

    boolean cya(List<Entry<RingMember, RingHost>> neighbors) {
        VersionedRing existingRing = this.versionedRing.get();
        VersionedRing updatedRing = ensureVersionedRing(neighbors);
        return existingRing != updatedRing; // reference equality is OK
    }

    public void expunged(Set<VersionedPartitionName> versionedPartitionNames) {
        for (VersionedPartitionName versionedPartitionName : versionedPartitionNames) {
            partitionCoordinators.remove(versionedPartitionName);
        }
    }

    void update(List<Entry<RingMember, RingHost>> neighbors, VersionedPartitionName versionedPartitionName, Status status, long txId) throws Exception {
        VersionedRing ring = ensureVersionedRing(neighbors);
        TakeVersionedPartitionCoordinator coordinator = partitionCoordinators.computeIfAbsent(versionedPartitionName,
            key -> new TakeVersionedPartitionCoordinator(versionedPartitionName,
                timestampedOrderIdProvider,
                status,
                new AtomicLong(txId),
                slowTakeInMillis,
                idPacker.pack(slowTakeInMillis, 0, 0), //TODO need orderIdProvider.deltaMillisToIds()
                systemReofferDeltaMillis,
                reofferDeltaMillis));
        PartitionProperties properties = versionedPartitionProvider.getProperties(versionedPartitionName.getPartitionName());
        coordinator.updateTxId(ring, status, txId, properties.takeFromFactor);
    }

    long availableRowsStream(RingMember ringMember, long takeSessionId, AvailableStream availableStream) throws Exception {
        long suggestedWaitInMillis = Long.MAX_VALUE;
        VersionedRing ring = versionedRing.get();
        for (TakeVersionedPartitionCoordinator coordinator : partitionCoordinators.values()) {
            PartitionProperties properties = versionedPartitionProvider.getProperties(coordinator.versionedPartitionName.getPartitionName());
            suggestedWaitInMillis = Math.min(suggestedWaitInMillis,
                coordinator.availableRowsStream(takeSessionId, ring, ringMember, properties.takeFromFactor, availableStream));
        }
        return suggestedWaitInMillis;
    }

    void rowsTaken(RingMember remoteRingMember, VersionedPartitionName localVersionedPartitionName, long localTxId) throws Exception {
        TakeVersionedPartitionCoordinator coordinator = partitionCoordinators.get(localVersionedPartitionName);
        if (coordinator != null) {
            PartitionProperties properties = versionedPartitionProvider.getProperties(coordinator.versionedPartitionName.getPartitionName());
            coordinator.rowsTaken(versionedRing.get(), remoteRingMember, localTxId, properties.takeFromFactor);
        }
    }

    private VersionedRing ensureVersionedRing(List<Entry<RingMember, RingHost>> neighbors) {
        return versionedRing.updateAndGet((existing) -> {
            if (existing.isStillValid(neighbors)) {
                return existing;
            } else {
                //LOG.info("RESIZED RING:" + ringName + " size:" + neighbors.size() + " " + this + " " + existing);
                return new VersionedRing(neighbors);
            }
        });
    }

    static public class VersionedRing {

        final int takeFromFactor;
        final LinkedHashMap<RingMember, Integer> members;

        public VersionedRing(List<Entry<RingMember, RingHost>> neighbors) {

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

    @Override
    public String toString() {
        return "TakeRingCoordinator{"
            + "ringName='" + new String(ringName) + '\''
            + '}';
    }
}
