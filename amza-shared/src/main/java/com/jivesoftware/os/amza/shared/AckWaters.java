package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class AckWaters {

    private final ConcurrentHashMap<RingHost, ConcurrentHashMap<VersionedPartitionName, Long>> ackWaters = new ConcurrentHashMap<>();
    private final AtomicLong[] changedLocks = new AtomicLong[1024]; // TODO expose to config.

    public AckWaters() {
        for (int i = 0; i < changedLocks.length; i++) {
            changedLocks[i] = new AtomicLong();
        }
    }

    public void set(RingHost ringHost, VersionedPartitionName partitionName, Long txId) {
        ConcurrentHashMap<VersionedPartitionName, Long> partitionTxIds = ackWaters.computeIfAbsent(ringHost, (RingHost t) -> {
            return new ConcurrentHashMap<>();
        });
        AtomicLong changedLock = getChangedLock(partitionName);
        changedLock.incrementAndGet();
        long merge = partitionTxIds.merge(partitionName, txId, Math::max);
        if (merge == txId) {
            synchronized (changedLock) {
                changedLock.notifyAll();
            }
        }
    }

    public Long get(RingHost ringHost, VersionedPartitionName partitionName) {
        ConcurrentHashMap<VersionedPartitionName, Long> partitionTxIds = ackWaters.get(ringHost);
        if (partitionTxIds == null) {
            return null;
        }
        return partitionTxIds.get(partitionName);
    }

    public AtomicLong getChangedLock(VersionedPartitionName partitionName) {
        return changedLocks[Math.abs(partitionName.hashCode()) % changedLocks.length];
    }

    public int await(VersionedPartitionName partitionName,
        long desiredTxId,
        Collection<RingHost> takeOrderHosts,
        int desiredTakeQuorum,
        long toMillis) throws Exception {

        RingHost[] ringHosts = takeOrderHosts.toArray(new RingHost[takeOrderHosts.size()]);
        int passed = 0;
        long startTime = System.currentTimeMillis();
        do {
            AtomicLong changedLock = getChangedLock(partitionName);
            long version = changedLock.get();
            for (int i = 0; i < ringHosts.length; i++) {
                RingHost ringHost = ringHosts[i];
                if (ringHost == null) {
                    continue;
                }
                Long txId = get(ringHost, partitionName);
                if (txId != null && txId >= desiredTxId) {
                    passed++;
                    ringHosts[i] = null;
                }
                if (passed >= desiredTakeQuorum) {
                    return passed;
                }
            }
            synchronized (changedLock) {
                if (version == changedLock.get()) {
                    long elapse = System.currentTimeMillis() - startTime;
                    if (passed < desiredTakeQuorum && elapse < toMillis) {
                        changedLock.wait(toMillis - elapse);
                    }
                }
            }
        } while (System.currentTimeMillis() - startTime < toMillis);
        return passed;
    }
}
