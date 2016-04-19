package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

/**
 * @author jonathan.colt
 */
public class CallableTransactor {

    private final Semaphore[] semaphores;
    private final int numPermits;

    public CallableTransactor(int numSemaphores, int numPermits) {
        this.semaphores = new Semaphore[numSemaphores];
        for (int i = 0; i < numSemaphores; i++) {
            this.semaphores[i] = new Semaphore(numPermits, true);
        }
        this.numPermits = numPermits;
    }

    public <R> R doWithOne(PartitionName partitionName,
        Callable<R> tx) throws Exception {

        return doWith(partitionName, 1, tx);
    }

    public <R> R doWithAll(PartitionName partitionName,
        Callable<R> tx) throws Exception {

        return doWith(partitionName, numPermits, tx);
    }

    private <R> R doWith(PartitionName partitionName,
        int count,
        Callable<R> tx) throws Exception {

        Semaphore semaphore = semaphore(partitionName);
        semaphore.acquire(count);
        try {
            return tx.call();
        } finally {
            semaphore.release(count);
        }
    }

    private Semaphore semaphore(PartitionName partitionName) {
        return semaphores[Math.abs(partitionName.hashCode() % semaphores.length)];
    }


}
