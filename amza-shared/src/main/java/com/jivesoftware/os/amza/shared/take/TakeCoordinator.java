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
import com.jivesoftware.os.amza.shared.take.RowsTaker.AvailableStream;
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

    private final TimestampedOrderIdProvider timestampedOrderIdProvider;

    private final ConcurrentHashMap<String, TakeRingCoordinator> takeRingCoordinators = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RingMember, Object> ringMembersLocks = new ConcurrentHashMap<>();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong cyaLock = new AtomicLong();
    private final long heartBeatIntervalMillis = 1_000; //TODO config

    public TakeCoordinator(TimestampedOrderIdProvider timestampedOrderIdProvider) {
        this.timestampedOrderIdProvider = timestampedOrderIdProvider;
    }

    public void awakeCya() {
        cyaLock.incrementAndGet();
        synchronized (cyaLock) {
            cyaLock.notifyAll();
        }
    }

    public void cya(AmzaRingReader ringReader, VersionedPartitionProvider partitionProvider) {
        long cyaIntervalMillis = 1_000; // TODO config
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
        return takeRingCoordinators.computeIfAbsent(ringName, (key) -> {
            return new TakeRingCoordinator(ringName, timestampedOrderIdProvider, neighbors);
        });
    }

    public void availableRowsStream(AmzaRingReader ringReader,
        RingMember remoteRingMember,
        long takeSessionId,
        long timeoutMillis,
        AvailableStream availableStream) throws Exception {

        AvailableStream watchAvailableStream = (versionedPartitionName, status, txId) -> {
            if (versionedPartitionName != null) {
                LOG.info("OFFER:local:{} partition:{} status:{} txId:{} to remote:{}",
                    ringReader.getRingMember(), versionedPartitionName, status, txId, remoteRingMember);
            }
            availableStream.available(versionedPartitionName, status, txId);
        };

        while (true) {
            long start = updates.get();
            //LOG.info("CHECKING:remote:{} local:{}", remoteRingMember, ringReader.getRingMember());

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
                suggestedWaitInMillis[0] = heartBeatIntervalMillis; // Hmmm
            }

            Object lock = ringMembersLocks.computeIfAbsent(remoteRingMember, (key) -> new Object());
            synchronized (lock) {
                long time = System.currentTimeMillis();
                long timeToWait = suggestedWaitInMillis[0];
                while (start == updates.get() && System.currentTimeMillis() - time < suggestedWaitInMillis[0]) {
                    long wait = Math.min(timeToWait, heartBeatIntervalMillis);
                    //LOG.info("PARKED:remote:{} for {}millis on local:{}",
                    //    remoteRingMember, wait, ringReader.getRingMember());
                    lock.wait(wait);
                    watchAvailableStream.available(null, null, 0); // Ping aka keep the socket alive
                    timeToWait -= heartBeatIntervalMillis;
                    if (timeToWait < 0) {
                        break;
                    }
                }
            }
        }
    }

    public void rowsTaken(RingMember remoteRingMember,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId) {

        LOG.info("TAKEN remote:{} took local:{} txId:{} partition:{}",
            remoteRingMember, null, localTxId, localVersionedPartitionName);

        String ringName = localVersionedPartitionName.getPartitionName().getRingName();
        TakeRingCoordinator ring = takeRingCoordinators.get(ringName);
        ring.rowsTaken(remoteRingMember, localVersionedPartitionName, localTxId);
    }

}
