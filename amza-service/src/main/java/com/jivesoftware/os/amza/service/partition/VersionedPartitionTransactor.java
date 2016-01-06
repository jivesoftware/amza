package com.jivesoftware.os.amza.service.partition;

import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
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

    public <R> R doWithOne(VersionedAquarium versionedAquarium,
        PartitionTx<R> tx) throws Exception {

        return doWith(versionedAquarium, 1, tx);
    }

    public <R> R doWithAll(VersionedAquarium versionedAquarium,
        PartitionTx<R> tx) throws Exception {

        return doWith(versionedAquarium, numPermits, tx);
    }

    private <R> R doWith(VersionedAquarium versionedAquarium,
        int count,
        PartitionTx<R> tx) throws Exception {

        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        Semaphore semaphore = semaphore(versionedPartitionName);
        semaphore.acquire(count);
        try {
            return tx.tx(versionedAquarium);
        } finally {
            semaphore.release(count);
        }
    }

    private Semaphore semaphore(VersionedPartitionName versionedPartitionName) {
        return semaphores[Math.abs(versionedPartitionName.hashCode() % semaphores.length)];
    }

}
