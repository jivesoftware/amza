package com.jivesoftware.os.amza.service.take;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.StripeTx.TxPartitionStripe;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.amza.service.take.TakeCoordinator.CategoryStream;
import com.jivesoftware.os.amza.service.take.TakeCoordinator.TookLatencyStream;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jonathan.colt
 */
public class TakeRingCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final SystemWALStorage systemWALStorage;
    private final RingMember rootMember;
    private final byte[] ringName;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final IdPacker idPacker;
    private final VersionedPartitionProvider versionedPartitionProvider;
    private final long slowTakeInMillis;
    private final long systemReofferDeltaMillis;
    private final long reofferDeltaMillis;

    private final ConcurrentHashMap<VersionedPartitionName, TakeVersionedPartitionCoordinator> partitionCoordinators = new ConcurrentHashMap<>();

    private volatile long callCount;
    private volatile VersionedRing versionedRing;

    public TakeRingCoordinator(SystemWALStorage systemWALStorage,
        RingMember rootMember,
        byte[] ringName,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        IdPacker idPacker,
        VersionedPartitionProvider versionedPartitionProvider,
        long systemReofferDeltaMillis,
        long slowTakeInMillis,
        long reofferDeltaMillis,
        RingTopology ring) {

        this.systemWALStorage = systemWALStorage;
        this.rootMember = rootMember;
        this.ringName = ringName;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.idPacker = idPacker;
        this.versionedPartitionProvider = versionedPartitionProvider;
        this.slowTakeInMillis = slowTakeInMillis;
        this.systemReofferDeltaMillis = systemReofferDeltaMillis;
        this.reofferDeltaMillis = reofferDeltaMillis;
        this.versionedRing = VersionedRing.compute(ring);
    }

    boolean cya(RingTopology ring) {
        VersionedRing existingRing = versionedRing;
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
        coordinator.updateTxId(versionedRing, properties.replicated, txId, invalidateOnline);
    }

    private TakeVersionedPartitionCoordinator ensureCoordinator(VersionedPartitionName versionedPartitionName) {
        return partitionCoordinators.computeIfAbsent(versionedPartitionName,
            key -> new TakeVersionedPartitionCoordinator(systemWALStorage,
                rootMember,
                versionedPartitionName,
                timestampedOrderIdProvider,
                slowTakeInMillis,
                idPacker.pack(slowTakeInMillis, 0, 0), //TODO need orderIdProvider.deltaMillisToIds()
                systemReofferDeltaMillis,
                reofferDeltaMillis));
    }

    long availableRowsStream(PartitionStripeProvider partitionStripeProvider,
        RingMember ringMember,
        long takeSessionId,
        AvailableStream availableStream) throws Exception {

        callCount++;
        long suggestedWaitInMillis = Long.MAX_VALUE;
        VersionedRing ring = versionedRing;
        for (TakeVersionedPartitionCoordinator coordinator : partitionCoordinators.values()) {
            coordinator.versionedPartitionProperties = versionedPartitionProvider.getVersionedProperties(coordinator.versionedPartitionName.getPartitionName(),
                coordinator.versionedPartitionProperties);
            PartitionProperties properties = coordinator.versionedPartitionProperties.properties;
            if (properties.replicated) {
                long timeout = coordinator.availableRowsStream(partitionStripeProvider,
                    takeSessionId,
                    ring,
                    ringMember,
                    availableStream);
                suggestedWaitInMillis = Math.min(suggestedWaitInMillis, timeout);
            }
        }
        return suggestedWaitInMillis;
    }

    void rowsTaken(RingMember remoteRingMember,
        long takeSessionId,
        TxPartitionStripe txPartitionStripe,
        VersionedAquarium versionedAquarium,
        long localTxId) throws Exception {
        TakeVersionedPartitionCoordinator coordinator = partitionCoordinators.get(versionedAquarium.getVersionedPartitionName());
        if (coordinator != null) {
            PartitionProperties properties = versionedPartitionProvider.getProperties(coordinator.versionedPartitionName.getPartitionName());
            coordinator.rowsTaken(takeSessionId,
                txPartitionStripe,
                versionedAquarium,
                versionedRing,
                remoteRingMember,
                localTxId,
                properties.replicated);
        }
    }

    private VersionedRing ensureVersionedRing(RingTopology ring) {
        if (!versionedRing.isStillValid(ring)) {
            synchronized (this) {
                if (!versionedRing.isStillValid(ring)) {
                    versionedRing = VersionedRing.compute(ring);
                }
            }
        }
        return versionedRing;
    }

    public long getCallCount() {
        return callCount;
    }

    boolean streamCategories(CategoryStream stream) throws Exception {
        for (TakeVersionedPartitionCoordinator partitionCoordinator : partitionCoordinators.values()) {
            long ringCallCount = getCallCount();
            long partitionCallCount = partitionCoordinator.getCallCount();
            if (!stream.stream(partitionCoordinator.versionedPartitionName, partitionCoordinator.currentCategory.get(), ringCallCount, partitionCallCount)) {
                return false;
            }
        }
        return true;
    }

    boolean streamTookLatencies(VersionedPartitionName versionedPartitionName, TookLatencyStream stream) throws Exception {
        TakeVersionedPartitionCoordinator partitionCoordinator = partitionCoordinators.get(versionedPartitionName);
        return (partitionCoordinator != null) && partitionCoordinator.streamTookLatencies(versionedRing, stream);
    }

    public long getPartitionCallCount(VersionedPartitionName versionedPartitionName) {
        TakeVersionedPartitionCoordinator partitionCoordinator = partitionCoordinators.get(versionedPartitionName);
        if (partitionCoordinator != null) {
            return partitionCoordinator.getCallCount();
        } else {
            return -1;
        }
    }

    static public class VersionedRing {

        final RingTopology ring;
        final int takeFromFactor;
        final LinkedHashMap<RingMember, Integer> members;

        private VersionedRing(RingTopology ring, int takeFromFactor, LinkedHashMap<RingMember, Integer> members) {
            this.ring = ring;
            this.takeFromFactor = takeFromFactor;
            this.members = members;
        }

        public static VersionedRing compute(RingTopology ring) {
            int ringSize = ring.entries.size();
            int neighborsSize = ringSize - (ring.rootMemberIndex == -1 ? 0 : 1);
            RingMember[] ringMembers = new RingMember[neighborsSize];
            for (int i = ring.rootMemberIndex + 1, j = 0; j < neighborsSize; i++, j++) {
                ringMembers[j] = ring.entries.get(i % ringSize).ringMember;
            }

            LinkedHashMap<RingMember, Integer> members = new LinkedHashMap<>();

            int takeFromFactor = ring.getTakeFromFactor();
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

            return new VersionedRing(ring, takeFromFactor, members);
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
