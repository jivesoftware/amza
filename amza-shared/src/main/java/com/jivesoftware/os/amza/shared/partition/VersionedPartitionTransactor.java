package com.jivesoftware.os.amza.shared.partition;

import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.aquarium.Waterline;
import java.util.concurrent.Semaphore;

/**
 * @author jonathan.colt
 */
public class VersionedPartitionTransactor {

    private final Semaphore[] semaphores;
    private final int numPermits;

    public VersionedPartitionTransactor(int numSemaphores, int numPermits) {
        this.semaphores = new Semaphore[numSemaphores];
        for (int i = 0; i < numSemaphores; i++) {
            this.semaphores[i] = new Semaphore(numPermits, true);
        }
        this.numPermits = numPermits;
    }

    public <R> R doWithOne(VersionedPartitionName versionedPartitionName,
        Waterline waterline,
        boolean isOnline,
        PartitionTx<R> tx) throws Exception {

        return doWith(versionedPartitionName, waterline, isOnline, 1, tx);
    }

    public <R> R doWithAll(VersionedPartitionName versionedPartitionName,
        Waterline waterline,
        boolean isOnline,
        PartitionTx<R> tx) throws Exception {

        return doWith(versionedPartitionName, waterline, isOnline, numPermits, tx);
    }

    private <R> R doWith(VersionedPartitionName versionedPartitionName,
        Waterline waterline,
        boolean isOnline,
        int count,
        PartitionTx<R> tx) throws Exception {

        Semaphore semaphore = semaphore(versionedPartitionName);
        semaphore.acquire(count);
        try {
            return tx.tx(versionedPartitionName, waterline, isOnline);
        } finally {
            semaphore.release(count);
        }
    }

    private Semaphore semaphore(VersionedPartitionName versionedPartitionName) {
        return semaphores[Math.abs(versionedPartitionName.hashCode() % semaphores.length)];
    }

}
