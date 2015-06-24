package com.jivesoftware.os.amza.shared.take;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
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
                    SetMultimap<String, PartitionName> desired = HashMultimap.create();

                    for (VersionedPartitionName versionedPartitionName : partitionProvider.getAllPartitions()) {
                        desired.put(versionedPartitionName.getPartitionName().getRingName(), versionedPartitionName.getPartitionName());
                    }

                    takeRingCoordinators.keySet().removeAll(Sets.difference(takeRingCoordinators.keySet(), desired.keySet()));
                    for (String ringName : desired.keySet()) {
                        TakeRingCoordinator takeRingCoordinator = takeRingCoordinators.get(ringName);
                        takeRingCoordinator.cya(desired.get(ringName));
                    }

                } catch (Exception x) {
                    LOG.error("Failed while ensuring aligment.");
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

    public void updated(AmzaRingReader ringReader, VersionedPartitionName versionedPartitionName, long txId) throws Exception {
        updates.incrementAndGet();
        String ringName = versionedPartitionName.getPartitionName().getRingName();
        Map.Entry<RingMember, RingHost>[] aboveRing = ringReader.getRingNeighbors(ringName).getAboveRing();
        TakeRingCoordinator ring = takeRingCoordinators.computeIfAbsent(ringName, (key) -> {
            return new TakeRingCoordinator(timestampedOrderIdProvider, aboveRing);
        });
        ring.update(aboveRing, versionedPartitionName, txId);

        for (Entry<RingMember, RingHost> r : aboveRing) {
            Object lock = ringMembersLocks.computeIfAbsent(r.getKey(), (RingMember t) -> new Object());
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    public void take(AmzaRingReader ringReader,
        RingMember ringMember,
        long takeSessionId,
        long timeoutMillis,
        PartitionUpdatedStream updatedPartitionsStream) throws Exception {
        while (true) {
            long start = updates.get();
            ringReader.getRingNames(ringMember, (ringName) -> {
                TakeRingCoordinator ring = takeRingCoordinators.get(ringName);
                ring.take(ringMember, takeSessionId, updatedPartitionsStream);
                return true;
            });

            Object lock = ringMembersLocks.computeIfAbsent(ringMember, (RingMember t) -> new Object());
            synchronized (lock) {
                if (start == updates.get()) {
                    lock.wait(timeoutMillis);
                }
            }
        }
    }

    public void took(RingMember ringMember, PartitionName partitionName, long txId) {
        TakeRingCoordinator ring = takeRingCoordinators.get(partitionName.getRingName());
        ring.took(ringMember, partitionName, txId);
    }

}
