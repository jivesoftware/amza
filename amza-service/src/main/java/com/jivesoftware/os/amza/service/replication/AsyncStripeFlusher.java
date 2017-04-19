package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public final class AsyncStripeFlusher implements Runnable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final int id;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong asyncVersion = new AtomicLong(0);
    private final AtomicLong forceVersion = new AtomicLong(0);
    private final AtomicLong asyncFlushedToVersion = new AtomicLong(0);
    private final AtomicLong forceFlushedToVersion = new AtomicLong(0);
    private final Object force = new Object();
    private final long asyncFlushIntervalMillis;
    private final Callable<Void> flushDelta;

    private final AtomicReference<HighwaterStorage> highwaterStorage = new AtomicReference<>();

    public AsyncStripeFlusher(int id,
        long asyncFlushIntervalMillis,
        Callable<Void> flushDelta) {

        this.id = id;
        this.asyncFlushIntervalMillis = asyncFlushIntervalMillis;
        this.flushDelta = flushDelta;
    }

    public void forceFlush(Durability durability, long waitForFlushInMillis) throws Exception {
        if (durability == Durability.ephemeral || durability == Durability.fsync_never) {
            return;
        }
        long waitForVersion = 0;
        AtomicLong flushedToVersion;
        if (durability == Durability.fsync_async) {
            waitForVersion = asyncVersion.incrementAndGet();
            flushedToVersion = asyncFlushedToVersion;
        } else if (durability == Durability.fsync_always) {
            waitForVersion = forceVersion.incrementAndGet();
            flushedToVersion = forceFlushedToVersion;
            synchronized (force) {
                force.notifyAll();
            }

            if (waitForFlushInMillis > 0) {
                long end = System.currentTimeMillis() + waitForFlushInMillis;
                while (waitForVersion > flushedToVersion.get()) {
                    synchronized (flushedToVersion) {
                        flushedToVersion.wait(Math.max(0, end - System.currentTimeMillis()));
                    }
                    if (end < System.currentTimeMillis()) {
                        throw new FailedToAchieveQuorumException("We couldn't fsync within " + waitForFlushInMillis + " millis.");
                    }
                }
            }
        } else {
            LOG.warn("Unsupported force flush for durability {}", durability);
        }
    }

    public void start(ExecutorService flusherExecutor, HighwaterStorage highwaterStorage) {
        if (running.compareAndSet(false, true)) {
            this.highwaterStorage.set(highwaterStorage);
            flusherExecutor.submit(this);
        }
    }

    public void stop() {
        running.compareAndSet(true, false);
    }

    @Override
    public void run() {
        try {
            long lastAsyncV = 0;
            long lastForcedV = 0;
            while (running.get()) {
                long asyncV = asyncVersion.get();
                long forcedV = forceVersion.get();

                if (lastAsyncV != asyncV || lastForcedV != forcedV) {
                    try {
                        flush();
                        lastAsyncV = asyncV;
                        lastForcedV = forcedV;
                    } catch (Throwable t) {
                        LOG.error("Excountered the following while flushing.", t);
                    }
                }

                if (lastAsyncV != asyncV) {
                    asyncFlushedToVersion.set(asyncV);
                    synchronized (asyncFlushedToVersion) {
                        asyncFlushedToVersion.notifyAll();
                    }

                }
                if (lastForcedV != forcedV) {
                    forceFlushedToVersion.set(forcedV);
                    synchronized (forceFlushedToVersion) {
                        forceFlushedToVersion.notifyAll();
                    }
                }

                synchronized (force) {
                    if (forceVersion.get() == forcedV) {
                        try {
                            force.wait(asyncFlushIntervalMillis);
                        } catch (InterruptedException ex) {
                            LOG.warn("Async flusher for {} was interrupted.", id);
                            return;
                        }
                    }
                }
            }
        } finally {
            running.set(false);
        }
    }

    private void flush() throws Exception {
        if (!highwaterStorage.get().flush(id, flushDelta)) {
            if (flushDelta != null) {
                flushDelta.call();
            }
        }
    }

}
