package com.jivesoftware.os.amza.shared.take;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.TxHighestPartitionTx;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.shared.ring.RingTopology;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
        long reofferDeltaMillis,
        RingTopology ring) {
        this.ringName = ringName;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.idPacker = idPacker;
        this.versionedPartitionProvider = versionedPartitionProvider;
        this.slowTakeInMillis = slowTakeInMillis;
        this.systemReofferDeltaMillis = systemReofferDeltaMillis;
        this.reofferDeltaMillis = reofferDeltaMillis;
        //LOG.info("INITIALIZED RING:" + ringName + " size:" + neighbors.size());
        this.versionedRing.compareAndSet(null, new VersionedRing(ring));
    }

    boolean cya(RingTopology ring) {
        VersionedRing existingRing = this.versionedRing.get();
        VersionedRing updatedRing = ensureVersionedRing(ring);
        return existingRing != updatedRing; // reference equality is OK
    }

    public void expunged(Set<VersionedPartitionName> versionedPartitionNames) {
        LOG.info("Remove partition coordinators for composted partitions: {}", versionedPartitionNames);
        for (VersionedPartitionName versionedPartitionName : versionedPartitionNames) {
            TakeVersionedPartitionCoordinator partitionCoordinator = partitionCoordinators.remove(versionedPartitionName);
            partitionCoordinator.expunged();
        }
    }

    void update(RingTopology ring,
        VersionedPartitionName versionedPartitionName,
        long txId) throws Exception {

        VersionedRing versionedRing = ensureVersionedRing(ring);
        TakeVersionedPartitionCoordinator coordinator = ensureCoordinator(versionedPartitionName);
        PartitionProperties properties = versionedPartitionProvider.getProperties(versionedPartitionName.getPartitionName());
        coordinator.updateTxId(versionedRing, properties.takeFromFactor, txId);
    }

    private TakeVersionedPartitionCoordinator ensureCoordinator(VersionedPartitionName versionedPartitionName) {
        return partitionCoordinators.computeIfAbsent(versionedPartitionName,
            key -> new TakeVersionedPartitionCoordinator(versionedPartitionName,
                timestampedOrderIdProvider,
                slowTakeInMillis,
                idPacker.pack(slowTakeInMillis, 0, 0), //TODO need orderIdProvider.deltaMillisToIds()
                systemReofferDeltaMillis,
                reofferDeltaMillis));
    }

    long availableRowsStream(TxHighestPartitionTx<Long> txHighestPartitionTx,
        RingMember ringMember,
        CheckState checkState,
        long takeSessionId,
        AvailableStream availableStream) throws Exception {

        long suggestedWaitInMillis = Long.MAX_VALUE;
        VersionedRing ring = versionedRing.get();
        for (TakeVersionedPartitionCoordinator coordinator : partitionCoordinators.values()) {
            if (!coordinator.isSteadyState(ringMember, takeSessionId)) {
                PartitionProperties properties = versionedPartitionProvider.getProperties(coordinator.versionedPartitionName.getPartitionName());
                suggestedWaitInMillis = Math.min(suggestedWaitInMillis,
                    coordinator.availableRowsStream(txHighestPartitionTx,
                        takeSessionId,
                        ring,
                        ringMember,
                        checkState.isOnline(ringMember, coordinator.versionedPartitionName),
                        properties.takeFromFactor,
                        availableStream));
            }
        }
        return suggestedWaitInMillis;
    }

    void rowsTaken(TxHighestPartitionTx<Long> txHighestPartitionTx,
        RingMember remoteRingMember,
        long takeSessionId,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId) throws Exception {
        TakeVersionedPartitionCoordinator coordinator = partitionCoordinators.get(localVersionedPartitionName);
        if (coordinator != null) {
            PartitionProperties properties = versionedPartitionProvider.getProperties(coordinator.versionedPartitionName.getPartitionName());
            coordinator.rowsTaken(txHighestPartitionTx, takeSessionId, versionedRing.get(), remoteRingMember, localTxId, properties.takeFromFactor);
        }
    }

    private VersionedRing ensureVersionedRing(RingTopology ring) {
        return versionedRing.updateAndGet((existing) -> {
            if (existing.isStillValid(ring)) {
                return existing;
            } else {
                //LOG.info("RESIZED RING:" + ringName + " size:" + neighbors.size() + " " + this + " " + existing);
                return new VersionedRing(ring);
            }
        });
    }

    static public class VersionedRing {

        final RingTopology ring;
        final int takeFromFactor;
        final LinkedHashMap<RingMember, Integer> members;

        public VersionedRing(RingTopology ring) {

            int ringSize = ring.entries.size();
            int neighborsSize = ringSize - (ring.rootMemberIndex == -1 ? 0 : 1);
            RingMember[] ringMembers = new RingMember[neighborsSize];
            for (int i = ring.rootMemberIndex + 1, j = 0; j < ringSize - 1; i++) {
                ringMembers[j] = ring.entries.get(i % ringSize).ringMember;
                j++;
            }

            this.ring = ring;
            this.members = new LinkedHashMap<>();
            this.takeFromFactor = 1 + (int) Math.sqrt(ringMembers.length);

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
                    members.put(ringMembers[memberIndex], category);
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

        boolean isStillValid(RingTopology ring) {
            return this.ring == ring;
        }
    }

    @Override
    public String toString() {
        return "TakeRingCoordinator{"
            + "ringName='" + new String(ringName) + '\''
            + '}';
    }
}
