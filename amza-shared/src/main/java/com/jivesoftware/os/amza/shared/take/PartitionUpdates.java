package com.jivesoftware.os.amza.shared.take;

import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker.PartitionUpdatedStream;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * TODO: Figure out how to do this "BETTER"!
 * @author jonathan.colt
 */
public class PartitionUpdates {

    private final ConcurrentHashMap<PartitionName, PartitionNameAndTxId> lastUpdate = new ConcurrentHashMap<>();
    private final NavigableMap<PartitionNameAndTxId, Long> partitionsLastestTxId = new ConcurrentSkipListMap<>();

    private final Object notifyChangeLock = new Object();
    private volatile long updates;

    public void updated(PartitionName partitionName, long txId) {
        PartitionNameAndTxId key = new PartitionNameAndTxId(partitionName, txId);
        PartitionNameAndTxId[] captureExisting = new PartitionNameAndTxId[1];
        PartitionNameAndTxId winner = lastUpdate.merge(partitionName, key, (PartitionNameAndTxId existing, PartitionNameAndTxId update) -> {
            captureExisting[0] = existing;
            return (existing != null && existing.txId >= update.txId) ? existing : update;
        });
        if (winner == key) {
            if (partitionsLastestTxId.putIfAbsent(winner, txId) == null) {
                synchronized (notifyChangeLock) {
                    updates++;
                    notifyChangeLock.notifyAll();
                }
            }
            if (captureExisting[0] != null) {
                partitionsLastestTxId.remove(captureExisting[0]);
            }
        }
    }

    public void take(long timeoutMillis, PartitionUpdatedStream updatedPartitionsStream) throws Exception {

        long anythingAfterThisTxId = 0;
        while (true) {
            long snapshot;
            synchronized (notifyChangeLock) {
                snapshot = updates;
            }
            long takenUpToThisTxId = anythingAfterThisTxId;
            for (PartitionNameAndTxId updated : partitionsLastestTxId.navigableKeySet()) {
                if (updated.txId <= takenUpToThisTxId) {
                    break;
                }
                updatedPartitionsStream.update(updated.partitionName, updated.txId);
                takenUpToThisTxId = Math.max(updated.txId, takenUpToThisTxId);
            }
            if (takenUpToThisTxId == anythingAfterThisTxId) {
                synchronized (notifyChangeLock) {
                    if (snapshot < updates) {
                        continue;
                    }
                    try {
                        notifyChangeLock.wait(timeoutMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (snapshot == updates) {
                        return;
                    }
                }
            } else {
                anythingAfterThisTxId = takenUpToThisTxId;
            }
        }
    }

    static class PartitionNameAndTxId implements Comparable<PartitionNameAndTxId> {

        private final PartitionName partitionName;
        private final long txId;

        public PartitionNameAndTxId(PartitionName partitionName, long txId) {
            this.partitionName = partitionName;
            this.txId = txId;
        }

        @Override
        public int compareTo(PartitionNameAndTxId o) {
            int i = -Long.compare(txId, o.txId);
            if (i != 0) {
                return i;
            }
            return partitionName.compareTo(o.partitionName);
        }

    }
}
