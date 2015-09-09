package com.jivesoftware.os.amza.shared.take;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.aquarium.Waterline;
import com.jivesoftware.os.amza.shared.partition.TxHighestPartitionTx;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class TakeCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final VersionedPartitionProvider versionedPartitionProvider;
    private final IdPacker idPacker;

    private final ConcurrentHashMap<IBA, TakeRingCoordinator> takeRingCoordinators = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RingMember, Object> ringMembersLocks = new ConcurrentHashMap<>();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong cyaLock = new AtomicLong();
    private final long cyaIntervalMillis;
    private final long slowTakeInMillis;
    private final long systemReofferDeltaMillis;
    private final long reofferDeltaMillis;

    public TakeCoordinator(AmzaStats amzaStats,
        TimestampedOrderIdProvider timestampedOrderIdProvider,
        IdPacker idPacker,
        VersionedPartitionProvider versionedPartitionProvider,
        long cyaIntervalMillis,
        long slowTakeInMillis,
        long systemReofferDeltaMillis,
        long reofferDeltaMillis) {
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

    public void start(AmzaRingReader ringReader) {
        ExecutorService cya = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cya-%d").build());
        cya.submit(() -> {
            while (true) {
                long updates = cyaLock.get();
                try {
                    for (IBA ringName : takeRingCoordinators.keySet()) {
                        TakeRingCoordinator takeRingCoordinator = takeRingCoordinators.get(ringName);
                        List<Entry<RingMember, RingHost>> neighbors = ringReader.getNeighbors(ringName.getBytes());
                        boolean neighborsChanged;
                        if (takeRingCoordinator != null) {
                            neighborsChanged = takeRingCoordinator.cya(neighbors);
                        } else {
                            ensureRingCoodinator(ringName.getBytes(), neighbors);
                            neighborsChanged = true;
                        }
                        if (neighborsChanged) {
                            awakeRemoteTakers(neighbors);
                        }
                    }

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

    public void expunged(List<VersionedPartitionName> versionedPartitionNames) {
        SetMultimap<IBA, VersionedPartitionName> expunged = HashMultimap.create();

        for (VersionedPartitionName versionedPartitionName : versionedPartitionNames) {
            expunged.put(new IBA(versionedPartitionName.getPartitionName().getRingName()), versionedPartitionName);
        }

        for (IBA ringName : expunged.keySet()) {
            TakeRingCoordinator takeRingCoordinator = takeRingCoordinators.get(ringName);
            if (takeRingCoordinator != null) {
                takeRingCoordinator.expunged(expunged.get(ringName));
            }
        }
    }

    public void updated(AmzaRingReader ringReader, VersionedPartitionName versionedPartitionName, Waterline waterline, boolean isOnline, long txId) throws
        Exception {
        updates.incrementAndGet();
        byte[] ringName = versionedPartitionName.getPartitionName().getRingName();
        List<Entry<RingMember, RingHost>> neighbors = ringReader.getNeighbors(ringName);
        TakeRingCoordinator ring = ensureRingCoodinator(ringName, neighbors);
        ring.update(neighbors, versionedPartitionName, waterline, isOnline, txId);
        amzaStats.updates(ringReader.getRingMember(), versionedPartitionName.getPartitionName(), 1, txId);
        awakeRemoteTakers(neighbors);
    }

    public void stateChanged(AmzaRingReader ringReader, VersionedPartitionName versionedPartitionName, Waterline waterline, boolean isOnline) throws Exception {
        updated(ringReader, versionedPartitionName, waterline, isOnline, 0);
    }

    private TakeRingCoordinator ensureRingCoodinator(byte[] ringName, List<Entry<RingMember, RingHost>> neighbors) {
        return takeRingCoordinators.computeIfAbsent(new IBA(ringName),
            key -> new TakeRingCoordinator(ringName,
                timestampedOrderIdProvider,
                idPacker,
                versionedPartitionProvider,
                slowTakeInMillis,
                systemReofferDeltaMillis,
                reofferDeltaMillis,
                neighbors));
    }

    private void awakeRemoteTakers(List<Entry<RingMember, RingHost>> neighbors) {
        for (Entry<RingMember, RingHost> r : neighbors) {
            Object lock = ringMembersLocks.computeIfAbsent(r.getKey(), (ringMember) -> new Object());
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    public void availableRowsStream(TxHighestPartitionTx<Long> txHighestPartitionTx,
        AmzaRingReader ringReader,
        RingMember remoteRingMember,
        long takeSessionId,
        long heartbeatIntervalMillis,
        AvailableStream availableStream,
        Callable<Void> deliverCallback,
        Callable<Void> pingCallback) throws Exception {

        AtomicLong offered = new AtomicLong();
        AvailableStream watchAvailableStream = (versionedPartitionName, txId) -> {
            //LOG.info("OFFER:local:{} remote:{} txId:{} partition:{} state:{}",
            //    ringReader.getRingMember(), remoteRingMember, txId, versionedPartitionName, state);
            offered.incrementAndGet();
            availableStream.available(versionedPartitionName, txId);
            amzaStats.offers(remoteRingMember, versionedPartitionName.getPartitionName(), 1, txId);
        };

        while (true) {
            long start = updates.get();
            //LOG.info("CHECKING: remote:{} local:{}", remoteRingMember, ringReader.getRingMember());

            long[] suggestedWaitInMillis = new long[]{Long.MAX_VALUE};
            ringReader.getRingNames(remoteRingMember, (ringName) -> {
                TakeRingCoordinator ring = takeRingCoordinators.get(new IBA(ringName));
                if (ring != null) {
                    suggestedWaitInMillis[0] = Math.min(suggestedWaitInMillis[0],
                        ring.availableRowsStream(txHighestPartitionTx, remoteRingMember, takeSessionId, watchAvailableStream));
                }
                return true;
            });
            if (suggestedWaitInMillis[0] == Long.MAX_VALUE) {
                suggestedWaitInMillis[0] = heartbeatIntervalMillis; // Hmmm
            }

            Object lock = ringMembersLocks.computeIfAbsent(remoteRingMember, (key) -> new Object());
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

    public void rowsTaken(RingMember remoteRingMember,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId,
        boolean isOnline) throws Exception {

        //LOG.info("TAKEN remote:{} took local:{} txId:{} partition:{}",
        //    remoteRingMember, null, localTxId, localVersionedPartitionName);
        byte[] ringName = localVersionedPartitionName.getPartitionName().getRingName();
        TakeRingCoordinator ring = takeRingCoordinators.get(new IBA(ringName));
        ring.rowsTaken(remoteRingMember, localVersionedPartitionName, localTxId, isOnline);
    }

}
