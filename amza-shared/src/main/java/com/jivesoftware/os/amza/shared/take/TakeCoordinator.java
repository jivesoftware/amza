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
import com.jivesoftware.os.amza.shared.take.UpdatesTaker.PartitionUpdatedStream;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
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

    public void cya(VersionedPartitionProvider partitionProvider) {
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
                        if (takeRingCoordinator != null) {
                            takeRingCoordinator.cya(desired.get(ringName));
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

    public void awakeCya() {
        cyaLock.incrementAndGet();
        synchronized (cyaLock) {
            cyaLock.notifyAll();
        }
    }

    public void updated(AmzaRingReader ringReader, VersionedPartitionName versionedPartitionName, Status status, long txId) throws Exception {
        updates.incrementAndGet();
        String ringName = versionedPartitionName.getPartitionName().getRingName();
        Map.Entry<RingMember, RingHost>[] aboveRing = ringReader.getRingNeighbors(ringName).getAboveRing();
        TakeRingCoordinator ring = ensureRingCoodinator(ringName, aboveRing);
        ring.update(aboveRing, versionedPartitionName, status, txId);

        for (Entry<RingMember, RingHost> r : aboveRing) {
            Object lock = ringMembersLocks.computeIfAbsent(r.getKey(), (ringMember) -> new Object());
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    private TakeRingCoordinator ensureRingCoodinator(String ringName, Entry<RingMember, RingHost>[] aboveRing) {
        return takeRingCoordinators.computeIfAbsent(ringName, (key) -> {
            return new TakeRingCoordinator(timestampedOrderIdProvider, aboveRing);
        });
    }

    public void take(AmzaRingReader ringReader,
        RingMember ringMember,
        long takeSessionId,
        long timeoutMillis,
        PartitionUpdatedStream updatedPartitionsStream) throws Exception {
        while (true) {
            long start = updates.get();

            long[] suggestedWaitInMillis = new long[]{Long.MAX_VALUE};
            ringReader.getRingNames(ringMember, (ringName) -> {
                TakeRingCoordinator ring = takeRingCoordinators.get(ringName);
                if (ring != null) {
                    suggestedWaitInMillis[0] = Math.min(suggestedWaitInMillis[0], ring.take(ringMember, takeSessionId, updatedPartitionsStream));
                }
                return true;
            });

            Object lock = ringMembersLocks.computeIfAbsent(ringMember, (key) -> new Object());
            synchronized (lock) {
                long time = System.currentTimeMillis();
                long timeToWait = suggestedWaitInMillis[0];
                while (start == updates.get() && System.currentTimeMillis() - time < suggestedWaitInMillis[0]) {
                    lock.wait(Math.min(timeToWait, heartBeatIntervalMillis));
                    updatedPartitionsStream.update(null, null, 0); // Ping aka keep the socket alive
                    timeToWait -= heartBeatIntervalMillis;
                    if (timeToWait < 0) {
                        break;
                    }
                }
            }
        }
    }

    public void remoteMemberTookToTxId(RingMember remoteRingMember,
        VersionedPartitionName remoteVersionedPartitionName,
        long localTxId) {
        TakeRingCoordinator ring = takeRingCoordinators.get(remoteVersionedPartitionName.getPartitionName().getRingName());
        ring.remoteMemberTookToTxId(remoteRingMember, remoteVersionedPartitionName, localTxId);
    }

}
