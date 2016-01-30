package com.jivesoftware.os.amza.service.take;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.value.ConcurrentBAHash;
import com.jivesoftware.os.amza.service.partition.TxHighestPartitionTx;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.PartitionStateStorage;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * @author jonathan.colt
 */
public class TakeCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RingMember rootMember;
    private final AmzaStats amzaStats;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final IdPacker idPacker;
    private final VersionedPartitionProvider versionedPartitionProvider;

    //private final ConcurrentHashMap<IBA, TakeRingCoordinator> takeRingCoordinators = new ConcurrentHashMap<>();
    private final ConcurrentBAHash<TakeRingCoordinator> takeRingCoordinators = new ConcurrentBAHash<>(13, true, 128);
    private final ConcurrentHashMap<RingMember, Object> ringMembersLocks = new ConcurrentHashMap<>();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong cyaLock = new AtomicLong();
    private final long cyaIntervalMillis;
    private final long slowTakeInMillis;
    private final long systemReofferDeltaMillis;
    private final long reofferDeltaMillis;

    public TakeCoordinator(RingMember rootMember,
        AmzaStats amzaStats,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        IdPacker idPacker,
        VersionedPartitionProvider versionedPartitionProvider,
        long cyaIntervalMillis,
        long slowTakeInMillis,
        long systemReofferDeltaMillis,
        long reofferDeltaMillis) {
        this.rootMember = rootMember;
        this.amzaStats = amzaStats;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.idPacker = idPacker;
        this.versionedPartitionProvider = versionedPartitionProvider;
        this.cyaIntervalMillis = cyaIntervalMillis;
        this.slowTakeInMillis = slowTakeInMillis;
        this.systemReofferDeltaMillis = systemReofferDeltaMillis;
        this.reofferDeltaMillis = reofferDeltaMillis;
    }

    //TODO bueller?
    public void awakeCya() {
        cyaLock.incrementAndGet();
        synchronized (cyaLock) {
            cyaLock.notifyAll();
        }
    }

    public interface BootstrapPartitions {

        boolean bootstrap(PartitionStream partitionStream) throws Exception;
    }

    public interface PartitionStream {

        boolean stream(VersionedPartitionName versionedPartitionName, LivelyEndState livelyEndState) throws Exception;
    }

    public void start(AmzaRingReader ringReader, BootstrapPartitions bootstrapPartitions) {
        ExecutorService cya = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cya-%d").build());
        cya.submit(() -> {
            while (true) {
                long updates = cyaLock.get();
                try {
                    takeRingCoordinators.stream((ringName, takeRingCoordinator) -> {
                        RingTopology ring = ringReader.getRing(ringName);
                        if (takeRingCoordinator.cya(ring)) {
                            awakeRemoteTakers(ring);
                        }
                        return true;
                    });

                } catch (Exception x) {
                    LOG.error("Failed while ensuring alignment.", x);
                }
                try {
                    synchronized (cyaLock) {
                        if (cyaLock.get() == updates) {
                            cyaLock.wait(cyaIntervalMillis);
                        }
                    }
                } catch (InterruptedException x) {
                    Thread.currentThread().interrupt();
                }
            }

        });
    }

    public void expunged(VersionedPartitionName versionedPartitionName) {
        TakeRingCoordinator takeRingCoordinator = takeRingCoordinators.get(versionedPartitionName.getPartitionName().getRingName());
        if (takeRingCoordinator != null) {
            takeRingCoordinator.expunged(versionedPartitionName);
        }
    }

    public void update(AmzaRingReader ringReader, VersionedPartitionName versionedPartitionName, long txId) throws Exception {
        updateInternal(ringReader, versionedPartitionName, txId, false);
    }

    private void updateInternal(AmzaRingReader ringReader, VersionedPartitionName versionedPartitionName, long txId, boolean invalidateOnline) throws Exception {
        updates.incrementAndGet();
        byte[] ringName = versionedPartitionName.getPartitionName().getRingName();
        RingTopology ring = ringReader.getRing(ringName);
        ensureRingCoordinator(ringName, () -> ring).update(ring, versionedPartitionName, txId, invalidateOnline);
        amzaStats.updates(ringReader.getRingMember(), versionedPartitionName.getPartitionName(), 1, txId);
        awakeRemoteTakers(ring);
    }

    public void stateChanged(AmzaRingReader ringReader, VersionedPartitionName versionedPartitionName) throws Exception {
        updateInternal(ringReader, versionedPartitionName, 0, true);
    }

    public boolean streamTookLatencies(VersionedPartitionName versionedPartitionName, TookLatencyStream stream) throws Exception {
        TakeRingCoordinator takeRingCoordinator = takeRingCoordinators.get(versionedPartitionName.getPartitionName().getRingName());
        return (takeRingCoordinator != null) && takeRingCoordinator.streamTookLatencies(versionedPartitionName, stream);
    }

    public interface TookLatencyStream {

        boolean stream(RingMember ringMember, long lastOfferedTxId, int category, long tooSlowTxId) throws Exception;
    }

    public interface CategoryStream {

        boolean stream(VersionedPartitionName versionedPartitionName, int category) throws Exception;
    }

    public boolean streamCategories(CategoryStream stream) throws Exception {
        return takeRingCoordinators.stream((ringName, takeRingCoordinator) -> {
            if (!takeRingCoordinator.streamCategories(stream)) {
                return false;
            }
            return true;
        });
    }

    interface RingSupplier {

        RingTopology get();
    }

    private TakeRingCoordinator ensureRingCoordinator(byte[] ringName, RingSupplier ringSupplier) {
        return takeRingCoordinators.computeIfAbsent(ringName,
            key -> new TakeRingCoordinator(rootMember,
                key,
                timestampedOrderIdProvider,
                idPacker,
                versionedPartitionProvider,
                systemReofferDeltaMillis,
                slowTakeInMillis,
                reofferDeltaMillis,
                ringSupplier.get()));
    }

    private static final Function<RingMember, Object> LOCK_CREATOR = (key) -> new Object();

    private void awakeRemoteTakers(RingTopology ring) {
        for (int i = 0; i < ring.entries.size(); i++) {
            if (ring.rootMemberIndex != i) {
                Object lock = ringMembersLocks.computeIfAbsent(ring.entries.get(i).ringMember, LOCK_CREATOR);
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }
    }

    public void availableRowsStream(TxHighestPartitionTx txHighestPartitionTx,
        AmzaRingReader ringReader,
        PartitionStateStorage partitionStateStorage,
        RingMember remoteRingMember,
        long takeSessionId,
        long heartbeatIntervalMillis,
        AvailableStream availableStream,
        Callable<Void> deliverCallback,
        Callable<Void> pingCallback) throws Exception {

        AtomicLong offered = new AtomicLong();
        AvailableStream watchAvailableStream = (versionedPartitionName, txId) -> {
            offered.incrementAndGet();
            availableStream.available(versionedPartitionName, txId);
            amzaStats.offers(remoteRingMember, versionedPartitionName.getPartitionName(), 1, txId);
        };

        while (true) {
            long start = updates.get();
          
            long[] suggestedWaitInMillis = new long[]{Long.MAX_VALUE};
            ringReader.getRingNames(remoteRingMember, (ringName) -> {
                TakeRingCoordinator ring = ensureRingCoordinator(ringName, () -> {
                    try {
                        return ringReader.getRing(ringName);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                if (ring != null) {
                    suggestedWaitInMillis[0] = Math.min(suggestedWaitInMillis[0],
                        ring.availableRowsStream(partitionStateStorage,
                            txHighestPartitionTx,
                            remoteRingMember,
                            takeSessionId,
                            watchAvailableStream));
                }
                return true;
            });
            if (suggestedWaitInMillis[0] == Long.MAX_VALUE) {
                suggestedWaitInMillis[0] = heartbeatIntervalMillis; // Hmmm
            }

            Object lock = ringMembersLocks.computeIfAbsent(remoteRingMember, LOCK_CREATOR);
            synchronized (lock) {
                long time = System.currentTimeMillis();
                long timeRemaining = suggestedWaitInMillis[0];
                while (start == updates.get() && System.currentTimeMillis() - time < suggestedWaitInMillis[0]) {
                    long timeToWait = Math.min(timeRemaining, heartbeatIntervalMillis);
                    //LOG.info("PARKED:remote:{} for {}millis on local:{}",
                    //    remoteRingMember, wait, ringReader.getRingMember());
                    if (offered.get() == 0) {
                        pingCallback.call(); // Ping aka keep the socket alive
                    } else {
                        deliverCallback.call();
                    }
                    lock.wait(timeToWait);
                    timeRemaining -= heartbeatIntervalMillis;
                    if (timeRemaining < 0) {
                        break;
                    }
                }
            }
        }
    }

    public void rowsTaken(TxHighestPartitionTx txHighestPartitionTx,
        RingMember remoteRingMember,
        long takeSessionId,
        VersionedAquarium versionedAquarium,
        long localTxId) throws Exception {

        byte[] ringName = versionedAquarium.getVersionedPartitionName().getPartitionName().getRingName();
        TakeRingCoordinator ring = takeRingCoordinators.get(ringName);
//        TakeRingCoordinator ring = takeRingCoordinators.get(new IBA(ringName));
        ring.rowsTaken(txHighestPartitionTx, remoteRingMember, takeSessionId, versionedAquarium, localTxId);
    }

}
