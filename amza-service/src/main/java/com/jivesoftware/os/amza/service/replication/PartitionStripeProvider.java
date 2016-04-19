package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.RemoteVersionedState;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.service.take.RowsTaker;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class PartitionStripeProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService compactDeltasThreadPool;
    private final ExecutorService flusherExecutor;
    private final PartitionStateStorage partitionStateStorage;
    private final PartitionStripe[] partitionStripes;
    private final HighwaterStorage[] highwaterStorages;
    private final ExecutorService[] rowTakerThreadPools;
    private final RowsTaker[] rowsTakers;
    private final AsyncStripeFlusher[] flushers;
    private final long deltaStripeCompactionIntervalInMillis;

    public PartitionStripeProvider(PartitionStateStorage partitionStateStorage,
        PartitionStripe[] partitionStripes,
        HighwaterStorage[] highwaterStorages,
        ExecutorService[] rowTakerThreadPools,
        RowsTaker[] rowsTakers,
        long asyncFlushIntervalMillis,
        long deltaStripeCompactionIntervalInMillis) {

        this.partitionStateStorage = partitionStateStorage;
        this.deltaStripeCompactionIntervalInMillis = deltaStripeCompactionIntervalInMillis;
        this.partitionStripes = Arrays.copyOf(partitionStripes, partitionStripes.length);
        this.highwaterStorages = Arrays.copyOf(highwaterStorages, highwaterStorages.length);
        this.rowTakerThreadPools = Arrays.copyOf(rowTakerThreadPools, rowTakerThreadPools.length);
        this.rowsTakers = Arrays.copyOf(rowsTakers, rowsTakers.length);

        this.compactDeltasThreadPool = Executors.newFixedThreadPool(partitionStripes.length,
            new ThreadFactoryBuilder().setNameFormat("compact-deltas-%d").build());
        this.flusherExecutor = Executors.newFixedThreadPool(partitionStripes.length,
            new ThreadFactoryBuilder().setNameFormat("stripe-flusher-%d").build());

        this.flushers = new AsyncStripeFlusher[partitionStripes.length];
        for (int i = 0; i < partitionStripes.length; i++) {
            this.flushers[i] = new AsyncStripeFlusher(this.partitionStripes[i], this.highwaterStorages[i], asyncFlushIntervalMillis);
        }
    }

    public <R> R txPartition(PartitionName partitionName, StripeTx<R> tx) throws Exception {
        return partitionStateStorage.tx(partitionName,
            (versionedAquarium, stripeIndex) -> tx.tx(stripeIndex, partitionStripes[stripeIndex], highwaterStorages[stripeIndex], versionedAquarium));

    }

    public void flush(PartitionName partitionName, Durability durability, long waitForFlushInMillis) throws Exception {
        partitionStateStorage.tx(partitionName,
            (versionedAquarium, stripeIndex) -> {
                flushers[stripeIndex].forceFlush(durability, waitForFlushInMillis);
                return null;
            });
    }

    ExecutorService getRowTakerThreadPool(PartitionName partitionName) throws Exception {
        return partitionStateStorage.tx(partitionName,
            (versionedAquarium, stripeIndex) -> {
                return rowTakerThreadPools[stripeIndex];
            });

    }

    RowsTaker getRowsTaker(PartitionName partitionName) throws Exception {
        return partitionStateStorage.tx(partitionName,
            (versionedAquarium, stripeIndex) -> {
                return rowsTakers[stripeIndex];
            });

    }

    public void mergeAll(boolean force) {
        for (PartitionStripe stripe : partitionStripes) {
            stripe.merge(force);
        }
    }

    public void load() throws Exception {
        ExecutorService stripeLoaderThreadPool = Executors.newFixedThreadPool(partitionStripes.length,
            new ThreadFactoryBuilder().setNameFormat("load-stripes-%d").build());
        List<Future> futures = new ArrayList<>();
        for (PartitionStripe partitionStripe : partitionStripes) {
            futures.add(stripeLoaderThreadPool.submit(() -> {
                try {
                    partitionStripe.load(partitionStateStorage);
                } catch (Exception x) {
                    LOG.error("Failed while loading " + partitionStripe, x);
                    throw new RuntimeException(x);
                }
            }));
        }
        int index = 0;
        for (Future future : futures) {
            LOG.info("Waiting for stripe:{} to load...", partitionStripes[index]);
            try {
                future.get();
                index++;
            } catch (InterruptedException | ExecutionException x) {
                LOG.error("Failed to load stripe:{}.", partitionStripes[index], x);
                throw x;
            }
        }
        stripeLoaderThreadPool.shutdown();
        LOG.info("All stripes {} have been loaded.", partitionStripes.length);
    }

    public void start() {
        for (PartitionStripe partitionStripe : partitionStripes) {
            compactDeltasThreadPool.submit(() -> {
                while (true) {
                    try {
                        if (partitionStripe.mergeable()) {
                            partitionStripe.merge(false);
                        }
                        Object awakeCompactionLock = partitionStripe.getAwakeCompactionLock();
                        synchronized (awakeCompactionLock) {
                            awakeCompactionLock.wait(deltaStripeCompactionIntervalInMillis);
                        }
                    } catch (Throwable x) {
                        LOG.error("Compactor failed.", x);
                    }
                }
            });
        }

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

    // TODO - begin factor out 'Later'
    public void streamLocalAquariums(PartitionStateStorage.PartitionMemberStateStream stream) throws Exception {
        partitionStateStorage.streamLocalAquariums(stream);
    }

    public RemoteVersionedState getRemoteVersionedState(RingMember ringMember, PartitionName partitionName) throws Exception {
        return partitionStateStorage.getRemoteVersionedState(ringMember, partitionName);
    }

    public Waterline awaitLeader(PartitionName partitionName, long waitForLeaderElection) throws Exception {
        return partitionStateStorage.awaitLeader(partitionName, waitForLeaderElection);
    }

    void expunged(VersionedPartitionName expunged) throws Exception {
        partitionStateStorage.expunged(expunged);
    }
    // TODO - end factor out 'Later'

    public interface StripeTx<R> {

        R tx(int stripe, PartitionStripe partitionStripe, HighwaterStorage highwaterStorage, VersionedAquarium versionedAquarium) throws Exception;
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
