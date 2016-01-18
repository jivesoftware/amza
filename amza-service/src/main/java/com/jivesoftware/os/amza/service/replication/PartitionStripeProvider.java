package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.service.take.RowsTaker;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class PartitionStripeProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService flusherExecutor;
    private final PartitionStripeFunction partitionStripeFunction;
    private final PartitionStripe[] deltaStripes;
    private final HighwaterStorage[] highwaterStorages;
    private final ExecutorService[] rowTakerThreadPools;
    private final RowsTaker[] rowsTakers;
    private final AsyncStripeFlusher[] flushers;

    public PartitionStripeProvider(PartitionStripeFunction partitionStripeFunction,
        PartitionStripe[] deltaStripes,
        HighwaterStorage[] highwaterStorages,
        ExecutorService[] rowTakerThreadPools,
        RowsTaker[] rowsTakers,
        long asyncFlushIntervalMillis) {
        this.partitionStripeFunction = partitionStripeFunction;
        this.deltaStripes = Arrays.copyOf(deltaStripes, deltaStripes.length);
        this.highwaterStorages = Arrays.copyOf(highwaterStorages, highwaterStorages.length);
        this.rowTakerThreadPools = Arrays.copyOf(rowTakerThreadPools, rowTakerThreadPools.length);
        this.rowsTakers = Arrays.copyOf(rowsTakers, rowsTakers.length);

        this.flusherExecutor = Executors.newFixedThreadPool(deltaStripes.length,
            new ThreadFactoryBuilder().setNameFormat("stripe-flusher-%d").build());

        this.flushers = new AsyncStripeFlusher[deltaStripes.length];
        for (int i = 0; i < deltaStripes.length; i++) {
            this.flushers[i] = new AsyncStripeFlusher(this.deltaStripes[i], this.highwaterStorages[i], asyncFlushIntervalMillis);
        }
    }

    public <R> R txPartition(PartitionName partitionName, StripeTx<R> tx) throws Exception {
        Preconditions.checkArgument(!partitionName.isSystemPartition(), "No systems allowed.");
        int stripeIndex = partitionStripeFunction.stripe(partitionName);
        return tx.tx(deltaStripes[stripeIndex], highwaterStorages[stripeIndex]);
    }

    public void flush(PartitionName partitionName, Durability durability, long waitForFlushInMillis) throws Exception {
        int stripeIndex = partitionStripeFunction.stripe(partitionName);
        flushers[stripeIndex].forceFlush(durability, waitForFlushInMillis);
    }

    ExecutorService getRowTakerThreadPool(PartitionName partitionName) {
        int stripeIndex = partitionStripeFunction.stripe(partitionName);
        return rowTakerThreadPools[stripeIndex];
    }

    RowsTaker getRowsTaker(PartitionName partitionName) {
        int stripeIndex = partitionStripeFunction.stripe(partitionName);
        return rowsTakers[stripeIndex];
    }

    public void mergeAll(boolean force) {
        for (PartitionStripe stripe : deltaStripes) {
            stripe.merge(force);
        }
    }

    public void start() {
        for (AsyncStripeFlusher flusher : flushers) {
            flusher.start(flusherExecutor);
        }
    }

    public void stop() {
        for (AsyncStripeFlusher flusher : flushers) {
            flusher.stop();
        }

        flusherExecutor.shutdownNow();

        for (ExecutorService rowTakerThreadPool : rowTakerThreadPools) {
            rowTakerThreadPool.shutdownNow();
        }
    }

    public interface StripeTx<R> {

        R tx(PartitionStripe stripe, HighwaterStorage highwaterStorage) throws Exception;
    }

    private final static class AsyncStripeFlusher implements Runnable {

        private final PartitionStripe deltaStripe;
        private final HighwaterStorage highwaterStorage;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final AtomicLong asyncVersion = new AtomicLong(0);
        private final AtomicLong forceVersion = new AtomicLong(0);
        private final AtomicLong asyncFlushedToVersion = new AtomicLong(0);
        private final AtomicLong forceFlushedToVersion = new AtomicLong(0);
        private final Object force = new Object();
        private final long asyncFlushIntervalMillis;

        public AsyncStripeFlusher(PartitionStripe deltaStripe,
            HighwaterStorage highwaterStorage,
            long asyncFlushIntervalMillis) {

            this.deltaStripe = deltaStripe;
            this.highwaterStorage = highwaterStorage;
            this.asyncFlushIntervalMillis = asyncFlushIntervalMillis;
        }

        public void forceFlush(Durability durability, long waitForFlushInMillis) throws Exception {
            if (durability == Durability.ephemeral || durability == Durability.fsync_never) {
                return;
            }
            long waitForVersion = 0;
            AtomicLong flushedVersion;
            AtomicLong flushedToVersion;
            if (durability == Durability.fsync_async) {
                waitForVersion = asyncVersion.incrementAndGet();
                flushedVersion = asyncVersion;
                flushedToVersion = asyncFlushedToVersion;
            } else {
                waitForVersion = forceVersion.incrementAndGet();
                flushedVersion = forceVersion;
                flushedToVersion = forceFlushedToVersion;
                synchronized (force) {
                    force.notifyAll();
                }
            }

            if (waitForFlushInMillis > 0) {
                long end = System.currentTimeMillis() + asyncFlushIntervalMillis;
                while (waitForVersion > flushedVersion.get()) {
                    synchronized (flushedToVersion) {
                        flushedToVersion.wait(Math.max(0, end - System.currentTimeMillis()));
                    }
                    if (end < System.currentTimeMillis()) {
                        throw new FailedToAchieveQuorumException("We couldn't fsync within " + waitForFlushInMillis + " millis.");
                    }
                }
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
                                LOG.warn("Async flusher for {} was interrupted.", deltaStripe);
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
            highwaterStorage.flush(() -> {
                deltaStripe.flush(true); // TODO eval for correctness
                return null;
            });
        }

        private void start(ExecutorService flusherExecutor) {
            if (running.compareAndSet(false, true)) {
                flusherExecutor.submit(this);
            }
        }

    }

}
