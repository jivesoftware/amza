package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class AwaitNotify<K> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AtomicLong[] changedLocks;

    public AwaitNotify(int stripingLevel) {
        this.changedLocks = new AtomicLong[stripingLevel];
        for (int i = 0; i < stripingLevel; i++) {
            this.changedLocks[i] = new AtomicLong();
        }
    }

    private AtomicLong getChangedLock(K key) {
        return changedLocks[Math.abs(key.hashCode() % changedLocks.length)];
    }

    public <R> R awaitChange(K key, Callable<Optional<R>> awaiter, long timeoutMillis) throws Exception {
        long startTime = System.currentTimeMillis();
        do {
            AtomicLong changedLock = getChangedLock(key);
            long version = changedLock.get();
            Optional<R> result = awaiter.call();
            if (result != null) {
                return result.orNull();
            }
            synchronized (changedLock) {
                if (version == changedLock.get()) {
                    long elapse = System.currentTimeMillis() - startTime;

                    if (elapse < timeoutMillis) {
                        changedLock.wait(timeoutMillis - elapse);
                        LOG.debug("Woke from waiting after {}", System.currentTimeMillis() - startTime);
                    }
                }
            }
        }
        while (timeoutMillis < 0 || System.currentTimeMillis() - startTime < timeoutMillis);

        throw new TimeoutException("Timed out awaiting changes after ms: " + timeoutMillis);
    }

    public void notifyChange(K key, Callable<Boolean> change) throws Exception {
        AtomicLong changedLock = getChangedLock(key);
        changedLock.incrementAndGet();
        if (change.call()) {
            synchronized (changedLock) {
                changedLock.notifyAll();
            }
        }
    }
}
