package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.RemoteVersionedState;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.AwaitNotify;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionTransactor;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.service.take.TakeCoordinator;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
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
public class PartitionStripeProvider implements TxPartitionState {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService compactDeltasThreadPool;
    private final ExecutorService flusherExecutor;
    private final PartitionStripe[] partitionStripes;
    private final HighwaterStorage highwaterStorage;
    private final RingMember rootRingMember;
    private final AmzaRingStoreReader ringStoreReader;
    private final AmzaAquariumProvider aquariumProvider;
    private final StorageVersionProvider storageVersionProvider;
    private final TakeCoordinator takeCoordinator;
    private final VersionedPartitionTransactor transactor;
    private final AwaitNotify<PartitionName> awaitNotify;

    private final AsyncStripeFlusher[] flushers;
    private final long deltaStripeCompactionIntervalInMillis;

    public PartitionStripeProvider(
        PartitionStripe[] partitionStripes,
        HighwaterStorage highwaterStorage,
        RingMember rootRingMember,
        AmzaRingStoreReader ringStoreReader,
        AmzaAquariumProvider aquariumProvider,
        StorageVersionProvider storageVersionProvider,
        TakeCoordinator takeCoordinator,
        AwaitNotify<PartitionName> awaitNotify,
        long asyncFlushIntervalMillis,
        long deltaStripeCompactionIntervalInMillis) {

        this.deltaStripeCompactionIntervalInMillis = deltaStripeCompactionIntervalInMillis;
        this.partitionStripes = partitionStripes;
        this.highwaterStorage = highwaterStorage;
        this.rootRingMember = rootRingMember;
        this.ringStoreReader = ringStoreReader;
        this.aquariumProvider = aquariumProvider;
        this.storageVersionProvider = storageVersionProvider;
        this.takeCoordinator = takeCoordinator;
        this.transactor = new VersionedPartitionTransactor(1024, 1024); // TODO expose to config?
        this.awaitNotify = awaitNotify;

        this.compactDeltasThreadPool = Executors.newFixedThreadPool(partitionStripes.length,
            new ThreadFactoryBuilder().setNameFormat("compact-deltas-%d").build());
        this.flusherExecutor = Executors.newFixedThreadPool(partitionStripes.length,
            new ThreadFactoryBuilder().setNameFormat("stripe-flusher-%d").build());

        this.flushers = new AsyncStripeFlusher[partitionStripes.length];
        for (int i = 0; i < partitionStripes.length; i++) {
            this.flushers[i] = new AsyncStripeFlusher(this.partitionStripes[i], this.highwaterStorage, asyncFlushIntervalMillis);
        }
    }

    public void load() throws Exception {
        ExecutorService stripeLoaderThreadPool = Executors.newFixedThreadPool(partitionStripes.length,
            new ThreadFactoryBuilder().setNameFormat("load-stripes-%d").build());
        List<Future> futures = new ArrayList<>();
        for (PartitionStripe partitionStripe : partitionStripes) {
            futures.add(stripeLoaderThreadPool.submit(() -> {
                try {
                    partitionStripe.load(this);
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
    }

    public <R> R txPartition(PartitionName partitionName, StripeTx<R> tx) throws Exception {
        return tx(partitionName,
            (versionedAquarium1, stripeIndex) -> tx.tx(stripeIndex, partitionStripes[stripeIndex], highwaterStorage, versionedAquarium1));
    }

    @Override
    public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
        VersionedAquarium versionedAquarium;
        int stripe;
        if (partitionName.isSystemPartition()) {
            versionedAquarium = new VersionedAquarium(new VersionedPartitionName(partitionName, 0), null, 0);
            stripe = storageVersionProvider.getSystemStripe(partitionName);
        } else {
            StorageVersion storageVersion = storageVersionProvider.createIfAbsent(partitionName);
            stripe = storageVersionProvider.getCurrentStripe(storageVersion);
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
            versionedAquarium = new VersionedAquarium(versionedPartitionName, aquariumProvider, storageVersion.stripeVersion);
        }
        if (stripe == -1) {
            throw new IllegalStateException("Failed to compute stripe for " + partitionName);
        }
        return transactor.doWithOne(versionedAquarium, stripe, tx);
    }

    public void flush(PartitionName partitionName, Durability durability, long waitForFlushInMillis) throws Exception {
        tx(partitionName,
            (versionedAquarium, stripeIndex) -> {
                flushers[stripeIndex].forceFlush(durability, waitForFlushInMillis);
                return null;
            });
    }

    public void mergeAll(boolean force) {
        for (PartitionStripe stripe : partitionStripes) {
            stripe.merge(force);
        }
    }

    public RemoteVersionedState getRemoteVersionedState(RingMember remoteRingMember, PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new RemoteVersionedState(Waterline.ALWAYS_ONLINE, 0);
        }

        StorageVersion remoteStorageVersion = storageVersionProvider.getRemote(remoteRingMember, partitionName);
        if (remoteStorageVersion == null) {
            return null;
        }

        Waterline remoteState = aquariumProvider.getCurrentState(partitionName, remoteRingMember, remoteStorageVersion.partitionVersion);
        return new RemoteVersionedState(remoteState, remoteStorageVersion.partitionVersion);
    }

    public Waterline awaitLeader(PartitionName partitionName, long timeoutMillis) throws Exception {
        if (partitionName.isSystemPartition()) {
            return null;
        }

        if (ringStoreReader.isMemberOfRing(partitionName.getRingName())) {
            return tx(partitionName, (versionedAquarium, stripe) -> {
                Waterline leaderWaterline = versionedAquarium.awaitOnline(timeoutMillis).getLeaderWaterline();
                if (!aquariumProvider.isOnline(leaderWaterline)) {
                    versionedAquarium.wipeTheGlass();
                }
                return leaderWaterline;
            });
        } else {
            return aquariumProvider.remoteAwaitProbableLeader(partitionName, timeoutMillis);
        }
    }

    public interface PartitionMemberStateStream {

        boolean stream(PartitionName partitionName, RingMember ringMember, VersionedAquarium versionedAquarium, int stripe) throws Exception;
    }

    public void streamLocalAquariums(PartitionMemberStateStream stream) throws Exception {
        storageVersionProvider.streamLocal((partitionName, ringMember, storageVersion) -> {
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
            VersionedAquarium versionedAquarium = new VersionedAquarium(versionedPartitionName, aquariumProvider, storageVersion.stripeVersion);
            int stripe = stripe(partitionName, storageVersion);
            return transactor.doWithOne(versionedAquarium, stripe,
                (versionedAquarium1, stripe1) -> stream.stream(partitionName, ringMember, versionedAquarium1, stripe1));
        });
    }

    public void expunged(VersionedPartitionName versionedPartitionName) throws Exception {
        VersionedAquarium versionedAquarium = new VersionedAquarium(versionedPartitionName, aquariumProvider, -1);
        StorageVersion storageVersion = storageVersionProvider.lookupStorageVersion(versionedPartitionName.getPartitionName());
        int stripe = storageVersion == null ? -1 : storageVersionProvider.getCurrentStripe(storageVersion);

        if (stripe != -1) {
            LOG.info("Removing storage versions for composted partition: {} stripe:{}", versionedPartitionName, stripe);
            transactor.doWithAll(versionedAquarium, stripe, (versionedAquarium1, stripe1) -> {
                awaitNotify.notifyChange(versionedPartitionName.getPartitionName(), () -> {
                    versionedAquarium1.delete();
                    storageVersionProvider.remove(rootRingMember, versionedAquarium1.getVersionedPartitionName());
                    return true;
                });
                return null;
            });
            takeCoordinator.expunged(versionedPartitionName);
        } else {
            LOG.warn("Failed to locate storage versions for composted partition: {}", versionedPartitionName);
        }
    }

    private int stripe(PartitionName partitionName, StorageVersion storageVersion) {
        int stripe = -1;
        if (partitionName.isSystemPartition()) {
            stripe = storageVersionProvider.getSystemStripe(partitionName);
        } else {
            stripe = storageVersionProvider.getCurrentStripe(storageVersion);
        }
        return stripe;
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
