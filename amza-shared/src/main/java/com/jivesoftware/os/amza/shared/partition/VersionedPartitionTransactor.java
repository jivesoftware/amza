package com.jivesoftware.os.amza.shared.partition;

import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.aquarium.LivelyEndState;
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
        LivelyEndState livelyEndState,
        PartitionTx<R> tx) throws Exception {

        return doWith(versionedPartitionName, livelyEndState, 1, tx);
    }

    public <R> R doWithAll(VersionedPartitionName versionedPartitionName,
        LivelyEndState livelyEndState,
        PartitionTx<R> tx) throws Exception {

        return doWith(versionedPartitionName, livelyEndState, numPermits, tx);
    }

    private <R> R doWith(VersionedPartitionName versionedPartitionName,
        LivelyEndState livelyEndState,
        int count,
        PartitionTx<R> tx) throws Exception {

        Semaphore semaphore = semaphore(versionedPartitionName);
        semaphore.acquire(count);
        try {
            return tx.tx(versionedPartitionName, livelyEndState);
        } finally {
            semaphore.release(count);
        }
    }

    private Semaphore semaphore(VersionedPartitionName versionedPartitionName) {
        return semaphores[Math.abs(versionedPartitionName.hashCode() % semaphores.length)];
    }

}
