package com.jivesoftware.os.amza.shared.region;

import com.jivesoftware.os.amza.shared.region.TxRegionStatus.Status;
import java.util.concurrent.Semaphore;

/**
 *
 * @author jonathan.colt
 */
public class VersionedRegionTransactor {

    private final Semaphore[] semaphores;
    private final int numPermits;

    public VersionedRegionTransactor(int numSemaphores, int numPermits) {
        this.semaphores = new Semaphore[numSemaphores];
        for (int i = 0; i < numSemaphores; i++) {
            this.semaphores[i] = new Semaphore(numPermits, true);
        }
        this.numPermits = numPermits;
    }

    public <R> R doWithOne(VersionedRegionName versionedRegionName, Status status, RegionTx<R> tx) throws Exception {
        return doWith(versionedRegionName, status, 1, tx);
    }

    public <R> R doWithAll(VersionedRegionName versionedRegionName, Status status, RegionTx<R> tx) throws Exception {
        return doWith(versionedRegionName, status, numPermits, tx);
    }

    private <R> R doWith(VersionedRegionName versionedRegionName, Status status, int count, RegionTx<R> tx) throws Exception {
        Semaphore semaphore = semaphore(versionedRegionName);
        semaphore.acquire(count);
        try {
            return tx.tx(versionedRegionName, status);
        } finally {
            semaphore.release(count);
        }
    }

    private Semaphore semaphore(VersionedRegionName versionedRegionName) {
        return semaphores[Math.abs((versionedRegionName.hashCode()) % semaphores.length)];
    }

}
