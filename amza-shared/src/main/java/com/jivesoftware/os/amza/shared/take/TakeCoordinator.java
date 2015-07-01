package com.jivesoftware.os.amza.shared.take;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.RowsTaker.AvailableStream;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class TakeCoordinator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final TimestampedOrderIdProvider timestampedOrderIdProvider;
    private final IdPacker idPacker;

    private final ConcurrentHashMap<String, TakeRingCoordinator> takeRingCoordinators = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RingMember, Object> ringMembersLocks = new ConcurrentHashMap<>();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong cyaLock = new AtomicLong();
    private final long cyaIntervalMillis;
    private final long slowTakeInMillis;

    public TakeCoordinator(AmzaStats amzaStats, TimestampedOrderIdProvider timestampedOrderIdProvider,
        IdPacker idPacker,
        long cyaIntervalMillis,
        long slowTakeInMillis) {
        this.amzaStats = amzaStats;
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
        this.idPacker = idPacker;
        this.cyaIntervalMillis = cyaIntervalMillis;
        this.slowTakeInMillis = slowTakeInMillis;
    }

    //TODO bueller?
    public void awakeCya() {
        cyaLock.incrementAndGet();
        synchronized (cyaLock) {
            cyaLock.notifyAll();
        }
    }

    public void start(AmzaRingReader ringReader, VersionedPartitionProvider partitionProvider) {
        ExecutorService cya = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cya-%d").build());
        cya.submit(() -> {
            while (true) {
                long updates = cyaLock.get();
                try {
                    SetMultimap<String, VersionedPartitionName> desired = HashMultimap.create();

                    for (VersionedPartitionName versionedPartitionName : partitionProvider.getAllPartitions()) {
                        desired.put(versionedPartitionName.getPartitionName().getRingName(), versionedPartitionName);
                    }

                    takeRingCoordinators.keySet().removeAll(Sets.difference(takeRingCoordinators.keySet(), desired.keySet()));
                    for (String ringName : desired.keySet()) {
                        TakeRingCoordinator takeRingCoordinator = takeRingCoordinators.get(ringName);
                        List<Entry<RingMember, RingHost>> neighbors = ringReader.getNeighbors(ringName);
                        if (takeRingCoordinator != null) {
                            takeRingCoordinator.cya(neighbors, desired.get(ringName));
                        } else {
                            ensureRingCoodinator(ringName, neighbors);
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

    public void updated(AmzaRingReader ringReader, VersionedPartitionName versionedPartitionName, Status status, long txId) throws Exception {
        updates.incrementAndGet();
        String ringName = versionedPartitionName.getPartitionName().getRingName();
        List<Entry<RingMember, RingHost>> neighbors = ringReader.getNeighbors(ringName);
        TakeRingCoordinator ring = ensureRingCoodinator(ringName, neighbors);
        ring.update(neighbors, versionedPartitionName, status, txId);
        amzaStats.updates(ringReader.getRingMember(), versionedPartitionName.getPartitionName(), 1, txId);
        awakeRemoteTakers(neighbors);
    }

    private void awakeRemoteTakers(List<Entry<RingMember, RingHost>> neighbors) {
        for (Entry<RingMember, RingHost> r : neighbors) {
            Object lock = ringMembersLocks.computeIfAbsent(r.getKey(), (ringMember) -> new Object());
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    private TakeRingCoordinator ensureRingCoodinator(String ringName, List<Entry<RingMember, RingHost>> neighbors) {
        return takeRingCoordinators.computeIfAbsent(ringName,
            key -> new TakeRingCoordinator(ringName, timestampedOrderIdProvider, idPacker, slowTakeInMillis, neighbors));
    }

    public void availableRowsStream(AmzaRingReader ringReader,
        RingMember remoteRingMember,
        long takeSessionId,
        long timeoutMillis,
        AvailableStream availableStream) throws Exception {

        AtomicLong offered = new AtomicLong();
        AvailableStream watchAvailableStream = (versionedPartitionName, status, txId) -> {
            //LOG.info("OFFER:local:{} remote:{} txId:{} partition:{} status:{}",
            //    ringReader.getRingMember(), remoteRingMember, txId, versionedPartitionName, status);
            offered.incrementAndGet();
            availableStream.available(versionedPartitionName, status, txId);
            amzaStats.offers(remoteRingMember, versionedPartitionName.getPartitionName(), 1, txId);
        };

        while (true) {
            long start = updates.get();
            //LOG.info("CHECKING: remote:{} local:{}", remoteRingMember, ringReader.getRingMember());

            long[] suggestedWaitInMillis = new long[]{Long.MAX_VALUE};
            ringReader.getRingNames(remoteRingMember, (ringName) -> {
                TakeRingCoordinator ring = takeRingCoordinators.get(ringName);
                if (ring != null) {
                    suggestedWaitInMillis[0] = Math.min(suggestedWaitInMillis[0],
                        ring.availableRowsStream(remoteRingMember, takeSessionId, watchAvailableStream));
                }
                return true;
            });
            if (suggestedWaitInMillis[0] == Long.MAX_VALUE) {
                suggestedWaitInMillis[0] = timeoutMillis; // Hmmm
            }

            if (offered.get() != 0) {
                return;
            }

            Object lock = ringMembersLocks.computeIfAbsent(remoteRingMember, (key) -> new Object());
            synchronized (lock) {
                long timeToWait = suggestedWaitInMillis[0];
                if (start == updates.get()) {
                    //LOG.info("PARKED LONG POLL:remote:{} for {}millis on local:{}",
                    //    remoteRingMember, wait, ringReader.getRingMember());
                    lock.wait(Math.min(timeToWait, timeoutMillis));
                    if (start == updates.get()) {
                        return; // Long poll is over
                    }
                }
            }

        }
    }

    public void rowsTaken(RingMember remoteRingMember,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId) {

        //LOG.info("TAKEN remote:{} took local:{} txId:{} partition:{}",
        //    remoteRingMember, null, localTxId, localVersionedPartitionName);
        String ringName = localVersionedPartitionName.getPartitionName().getRingName();
        TakeRingCoordinator ring = takeRingCoordinators.get(ringName);
        ring.rowsTaken(remoteRingMember, localVersionedPartitionName, localTxId);
    }

}
