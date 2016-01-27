package com.jivesoftware.os.amza.service.take;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.partition.TxHighestPartitionTx;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.PartitionStateStorage;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.amza.service.take.TakeCoordinator.CategoryStream;
import com.jivesoftware.os.amza.service.take.TakeCoordinator.TookLatencyStream;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class TakeRingCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RingMember rootMember;
    private final byte[] ringName;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final IdPacker idPacker;
    private final VersionedPartitionProvider versionedPartitionProvider;
    private final long slowTakeInMillis;
    private final long systemReofferDeltaMillis;
    private final long reofferDeltaMillis;
    private final AtomicReference<VersionedRing> versionedRing = new AtomicReference<>();
    private final ConcurrentHashMap<VersionedPartitionName, TakeVersionedPartitionCoordinator> partitionCoordinators = new ConcurrentHashMap<>();

    public TakeRingCoordinator(RingMember rootMember,
        byte[] ringName,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        IdPacker idPacker,
        VersionedPartitionProvider versionedPartitionProvider,
        long systemReofferDeltaMillis,
        long slowTakeInMillis,
        long reofferDeltaMillis,
        RingTopology ring) {
        this.rootMember = rootMember;
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

    public void expunged(VersionedPartitionName versionedPartitionName) {
        TakeVersionedPartitionCoordinator partitionCoordinator = partitionCoordinators.remove(versionedPartitionName);
        if (partitionCoordinator != null) {
            LOG.info("Remove partition coordinator for composted partition: {}", versionedPartitionName);
            partitionCoordinator.expunged();
        }
    }

    void update(RingTopology ring,
        VersionedPartitionName versionedPartitionName,
        long txId,
        boolean invalidateOnline) throws Exception {

        VersionedRing versionedRing = ensureVersionedRing(ring);
        TakeVersionedPartitionCoordinator coordinator = ensureCoordinator(versionedPartitionName);
        PartitionProperties properties = versionedPartitionProvider.getProperties(versionedPartitionName.getPartitionName());
        coordinator.updateTxId(versionedRing, properties.takeFromFactor, txId, invalidateOnline);
    }

    private TakeVersionedPartitionCoordinator ensureCoordinator(VersionedPartitionName versionedPartitionName) {
        return partitionCoordinators.computeIfAbsent(versionedPartitionName,
            key -> new TakeVersionedPartitionCoordinator(rootMember,
                versionedPartitionName,
                timestampedOrderIdProvider,
                slowTakeInMillis,
                idPacker.pack(slowTakeInMillis, 0, 0), //TODO need orderIdProvider.deltaMillisToIds()
                systemReofferDeltaMillis,
                reofferDeltaMillis));
    }

    long availableRowsStream(PartitionStateStorage partitionStateStorage,
        TxHighestPartitionTx<Long> txHighestPartitionTx,
        RingMember ringMember,
        long takeSessionId,
        AvailableStream availableStream) throws Exception {

        long suggestedWaitInMillis = Long.MAX_VALUE;
        VersionedRing ring = versionedRing.get();
        for (TakeVersionedPartitionCoordinator coordinator : partitionCoordinators.values()) {
            PartitionProperties properties = versionedPartitionProvider.getProperties(coordinator.versionedPartitionName.getPartitionName());
            Long timeout = coordinator.availableRowsStream(partitionStateStorage,
                txHighestPartitionTx,
                takeSessionId,
                ring,
                ringMember,
                properties.takeFromFactor,
                availableStream);
            suggestedWaitInMillis = Math.min(suggestedWaitInMillis,
                timeout);
        }
        return suggestedWaitInMillis;
    }

    void rowsTaken(TxHighestPartitionTx<Long> txHighestPartitionTx,
        RingMember remoteRingMember,
        long takeSessionId,
        VersionedAquarium versionedAquarium,
        long localTxId) throws Exception {
        TakeVersionedPartitionCoordinator coordinator = partitionCoordinators.get(versionedAquarium.getVersionedPartitionName());
        if (coordinator != null) {
            PartitionProperties properties = versionedPartitionProvider.getProperties(coordinator.versionedPartitionName.getPartitionName());
            coordinator.rowsTaken(txHighestPartitionTx,
                takeSessionId,
                versionedAquarium,
                versionedRing.get(),
                remoteRingMember,
                localTxId,
                properties.takeFromFactor);
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

    boolean streamCategories(CategoryStream stream) throws Exception {
        for (TakeVersionedPartitionCoordinator partitionCoordinator : partitionCoordinators.values()) {
            if (!stream.stream(partitionCoordinator.versionedPartitionName, partitionCoordinator.currentCategory.get())) {
                return false;
            }
        }
        return true;
    }

    boolean streamTookLatencies(VersionedPartitionName versionedPartitionName, TookLatencyStream stream) throws Exception {
        TakeVersionedPartitionCoordinator partitionCoordinator = partitionCoordinators.get(versionedPartitionName);
        return (partitionCoordinator != null) && partitionCoordinator.streamTookLatencies(versionedRing.get(), stream);
    }

    static public class VersionedRing {

        final RingTopology ring;
        final int takeFromFactor;
        final LinkedHashMap<RingMember, Integer> members;

        public VersionedRing(RingTopology ring) {

            int ringSize = ring.entries.size();
            int neighborsSize = ringSize - (ring.rootMemberIndex == -1 ? 0 : 1);
            RingMember[] ringMembers = new RingMember[neighborsSize];
            for (int i = ring.rootMemberIndex + 1, j = 0; j < ringSize - 1; i++, j++) {
                ringMembers[j] = ring.entries.get(i % ringSize).ringMember;
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
