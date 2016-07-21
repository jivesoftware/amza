package com.jivesoftware.os.amza.service.take;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.StripeTx.TxPartitionStripe;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader.RingNameStream;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.jive.utils.collections.bah.BAHEqualer;
import com.jivesoftware.os.jive.utils.collections.bah.BAHMapState;
import com.jivesoftware.os.jive.utils.collections.bah.BAHState;
import com.jivesoftware.os.jive.utils.collections.bah.BAHash;
import com.jivesoftware.os.jive.utils.collections.bah.BAHasher;
import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class TakeCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final SystemWALStorage systemWALStorage;
    private final RingMember rootMember;
    private final AmzaStats amzaStats;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final IdPacker idPacker;
    private final VersionedPartitionProvider versionedPartitionProvider;

    private final ConcurrentBAHash<TakeRingCoordinator> takeRingCoordinators = new ConcurrentBAHash<>(13, true, 128);
    private final ConcurrentHashMap<RingMember, Object> systemRingMembersLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RingMember, Object> stripedRingMembersLocks = new ConcurrentHashMap<>();
    private final AtomicLong systemUpdates = new AtomicLong();
    private final AtomicLong stripedUpdates = new AtomicLong();
    private final AtomicLong cyaLock = new AtomicLong();
    private final long cyaIntervalMillis;
    private final long slowTakeInMillis;
    private final long systemReofferDeltaMillis;
    private final long reofferDeltaMillis;

    private final AtomicBoolean running = new AtomicBoolean();
    private ExecutorService cya;

    public TakeCoordinator(SystemWALStorage systemWALStorage,
        RingMember rootMember,
        AmzaStats amzaStats,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        IdPacker idPacker,
        VersionedPartitionProvider versionedPartitionProvider,
        long cyaIntervalMillis,
        long slowTakeInMillis,
        long systemReofferDeltaMillis,
        long reofferDeltaMillis) {
        this.systemWALStorage = systemWALStorage;
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
        if (running.compareAndSet(false, true)) {
            cya = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cya-%d").build());
            cya.submit(() -> {
                while (running.get()) {
                    long updates = cyaLock.get();
                    try {
                        takeRingCoordinators.stream((ringName, takeRingCoordinator) -> {
                            RingTopology ring = ringReader.getRing(ringName);
                            if (takeRingCoordinator.cya(ring)) {
                                // whatever
                                awakeRemoteTakers(ring, true);
                                awakeRemoteTakers(ring, false);
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
                    } catch (Exception x) {
                        LOG.warn("Exception while awaiting cya.", x);
                    }
                }
                return null;
            });
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            cya.shutdownNow();
            cya = null;
        }
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

    private void updateInternal(AmzaRingReader ringReader,
        VersionedPartitionName versionedPartitionName,
        long txId,
        boolean invalidateOnline) throws Exception {

        boolean system = versionedPartitionName.getPartitionName().isSystemPartition();
        if (system) {
            systemUpdates.incrementAndGet();
        } else {
            stripedUpdates.incrementAndGet();
        }
        byte[] ringName = versionedPartitionName.getPartitionName().getRingName();
        RingTopology ring = ringReader.getRing(ringName);
        ensureRingCoordinator(ringName, null, -1, () -> ring).update(ring, versionedPartitionName, txId, invalidateOnline);
        amzaStats.updates(ringReader.getRingMember(), versionedPartitionName.getPartitionName(), 1, txId);
        awakeRemoteTakers(ring, system);
    }

    public void stateChanged(AmzaRingReader ringReader, VersionedPartitionName versionedPartitionName) throws Exception {
        updateInternal(ringReader, versionedPartitionName, 0, true);
    }

    public long getRingCallCount(byte[] ringName) {
        TakeRingCoordinator ringCoordinator = takeRingCoordinators.get(ringName);
        if (ringCoordinator != null) {
            return ringCoordinator.getCallCount();
        } else {
            return -1;
        }
    }

    public long getPartitionCallCount(VersionedPartitionName versionedPartitionName) {
        TakeRingCoordinator ringCoordinator = takeRingCoordinators.get(versionedPartitionName.getPartitionName().getRingName());
        if (ringCoordinator != null) {
            return ringCoordinator.getPartitionCallCount(versionedPartitionName);
        } else {
            return -1;
        }
    }

    public interface TookLatencyStream {

        boolean stream(RingMember ringMember,
            long lastOfferedTxId,
            int category,
            long tooSlowTxId,
            long takeSessionId,
            boolean online,
            boolean steadyState,
            long lastOfferedMillis,
            long lastTakenMillis,
            long lastCategoryCheckMillis) throws Exception;
    }

    public boolean streamTookLatencies(VersionedPartitionName versionedPartitionName, TookLatencyStream stream) throws Exception {
        TakeRingCoordinator takeRingCoordinator = takeRingCoordinators.get(versionedPartitionName.getPartitionName().getRingName());
        return (takeRingCoordinator != null) && takeRingCoordinator.streamTookLatencies(versionedPartitionName, stream);
    }

    public interface CategoryStream {

        boolean stream(VersionedPartitionName versionedPartitionName, int category, long ringCallCount, long partitionCallCount) throws Exception;
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

    private TakeRingCoordinator ensureRingCoordinator(byte[] ringName, BAHash<TakeRingCoordinator> stackCache, int ringHash, RingSupplier ringSupplier) {
        TakeRingCoordinator ringCoordinator = stackCache == null ? null : stackCache.get(ringHash, ringName, 0, ringName.length);
        if (ringCoordinator == null) {
            ringCoordinator = takeRingCoordinators.computeIfAbsent(ringName,
                key -> new TakeRingCoordinator(systemWALStorage,
                    rootMember,
                    key,
                    timestampedOrderIdProvider,
                    idPacker,
                    versionedPartitionProvider,
                    systemReofferDeltaMillis,
                    slowTakeInMillis,
                    reofferDeltaMillis,
                    ringSupplier.get()));
            if (stackCache != null) {
                stackCache.put(ringHash, ringName, ringCoordinator);
            }
        }
        return ringCoordinator;
    }

    private static final Function<RingMember, Object> LOCK_CREATOR = (key) -> new Object();

    private void awakeRemoteTakers(RingTopology ring, boolean system) {
        ConcurrentHashMap<RingMember, Object> ringMembersLocks = system ? systemRingMembersLocks : stripedRingMembersLocks;
        for (int i = 0; i < ring.entries.size(); i++) {
            if (ring.rootMemberIndex != i) {
                Object lock = ringMembersLocks.computeIfAbsent(ring.entries.get(i).ringMember, LOCK_CREATOR);
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }
    }

    public void availableRowsStream(boolean system,
        AmzaRingReader ringReader,
        PartitionStripeProvider partitionStripeProvider,
        RingMember remoteRingMember,
        long takeSessionId,
        long heartbeatIntervalMillis,
        AvailableStream availableStream,
        Callable<Void> deliverCallback,
        Callable<Void> pingCallback) throws Exception {

        MutableLong offered = new MutableLong();
        AvailableStream watchAvailableStream = (versionedPartitionName, txId) -> {
            offered.add(1);
            availableStream.available(versionedPartitionName, txId);
            amzaStats.offers(remoteRingMember, versionedPartitionName.getPartitionName(), 1, txId);
        };

        int systemRingHash = BAHasher.SINGLETON.hashCode(AmzaRingReader.SYSTEM_RING, 0, AmzaRingReader.SYSTEM_RING.length);
        BAHash<TakeRingCoordinator> stackCache = new BAHash<>(
            new BAHMapState<>(takeRingCoordinators.size() * 2, true, BAHMapState.NIL),
            BAHasher.SINGLETON,
            BAHEqualer.SINGLETON);

        long[] suggestedWaitInMillis = new long[] { Long.MAX_VALUE };

        RingNameStream ringNameStream = (ringName, ringHash) -> {
            if (!system && Arrays.equals(ringName, AmzaRingReader.SYSTEM_RING)) {
                return true;
            }

            TakeRingCoordinator ring = ensureRingCoordinator(ringName, stackCache, ringHash, () -> {
                try {
                    return ringReader.getRing(ringName);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            if (ring != null) {
                suggestedWaitInMillis[0] = Math.min(suggestedWaitInMillis[0],
                    ring.availableRowsStream(partitionStripeProvider,
                        remoteRingMember,
                        takeSessionId,
                        watchAvailableStream));
            }
            return true;
        };

        AtomicLong updates = system ? systemUpdates : stripedUpdates;
        ConcurrentHashMap<RingMember, Object> ringMembersLocks = system ? systemRingMembersLocks : stripedRingMembersLocks;
        Object lock = ringMembersLocks.computeIfAbsent(remoteRingMember, LOCK_CREATOR);

        while (true) {
            long initialUpdates = updates.get();
            suggestedWaitInMillis[0] = Long.MAX_VALUE;

            long start = System.currentTimeMillis();
            if (system) {
                ringNameStream.stream(AmzaRingReader.SYSTEM_RING, systemRingHash);
            } else {
                ringReader.getRingNames(remoteRingMember, ringNameStream);
            }
            long elapsed = System.currentTimeMillis() - start;

            int offerPower = offered.longValue() == 0 ? -1 : UIO.chunkPower(offered.longValue(), 0);
            LOG.inc("takeCoordinator>" + (system ? "system" : "partition") + ">" + remoteRingMember.getMember() + ">count", 1);
            LOG.inc("takeCoordinator>" + (system ? "system" : "partition") + ">" + remoteRingMember.getMember() + ">elapsed", elapsed);
            LOG.inc("takeCoordinator>" + (system ? "system" : "partition") + ">" + remoteRingMember.getMember() + ">offered>" + offerPower, 1);

            if (offered.longValue() == 0) {
                pingCallback.call(); // Ping aka keep the socket alive
            } else {
                offered.setValue(0);
                deliverCallback.call();
            }

            if (suggestedWaitInMillis[0] == Long.MAX_VALUE) {
                suggestedWaitInMillis[0] = heartbeatIntervalMillis; // Hmmm
            }

            synchronized (lock) {
                long time = System.currentTimeMillis();
                long timeRemaining = suggestedWaitInMillis[0];
                while (initialUpdates == updates.get() && System.currentTimeMillis() - time < suggestedWaitInMillis[0]) {
                    long timeToWait = Math.min(timeRemaining, heartbeatIntervalMillis);
                    //LOG.info("PARKED:remote:{} for {}millis on local:{}",
                    //    remoteRingMember, wait, ringReader.getRingMember());
                    if (offered.longValue() == 0) {
                        pingCallback.call(); // Ping aka keep the socket alive
                    } else {
                        offered.setValue(0);
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

    public void rowsTaken(RingMember remoteRingMember,
        long takeSessionId,
        TxPartitionStripe txPartitionStripe,
        VersionedAquarium versionedAquarium,
        long localTxId) throws Exception {

        byte[] ringName = versionedAquarium.getVersionedPartitionName().getPartitionName().getRingName();
        TakeRingCoordinator ring = takeRingCoordinators.get(ringName);
        if (ring != null) {
            ring.rowsTaken(remoteRingMember, takeSessionId, txPartitionStripe, versionedAquarium, localTxId);
        }
        amzaStats.acks(remoteRingMember, versionedAquarium.getVersionedPartitionName().getPartitionName(), 1, localTxId);
    }

}
