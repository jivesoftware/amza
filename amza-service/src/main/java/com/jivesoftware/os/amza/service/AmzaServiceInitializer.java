/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.filer.DirectByteBufferFactory;
import com.jivesoftware.os.amza.service.replication.AmzaAquariumProvider;
import com.jivesoftware.os.amza.service.replication.AmzaAquariumProvider.AmzaLivelinessStorage;
import com.jivesoftware.os.amza.service.replication.PartitionBackedHighwaterStorage;
import com.jivesoftware.os.amza.service.replication.PartitionComposter;
import com.jivesoftware.os.amza.service.replication.PartitionStateStorage;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.service.replication.PartitionStripeFunction;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.PartitionTombstoneCompactor;
import com.jivesoftware.os.amza.service.replication.RowChangeTaker;
import com.jivesoftware.os.amza.service.replication.StorageVersionProvider;
import com.jivesoftware.os.amza.service.replication.StripedPartitionCommitChanges;
import com.jivesoftware.os.amza.service.replication.SystemPartitionCommitChanges;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.CacheId;
import com.jivesoftware.os.amza.service.ring.RingSet;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.MemoryBackedRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.service.storage.delta.DeltaStripeWALStorage;
import com.jivesoftware.os.amza.service.storage.delta.DeltaWALFactory;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.service.take.RowsTaker;
import com.jivesoftware.os.amza.service.take.RowsTakerFactory;
import com.jivesoftware.os.amza.service.take.TakeCoordinator;
import com.jivesoftware.os.aquarium.AtQuorum;
import com.jivesoftware.os.aquarium.Liveliness;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class AmzaServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static class AmzaServiceConfig {

        public String[] workingDirectories = new String[] { "./var/data/" };

        public long checkIfCompactionIsNeededIntervalInMillis = 60_000;
        public long compactTombstoneIfOlderThanNMillis = 30 * 24 * 60 * 60 * 1000L;

        public int numberOfCompactorThreads = 8;
        public int numberOfTakerThreads = 8;

        public int corruptionParanoiaFactor = 10;
        public int updatesBetweenLeaps = 4_096;
        public int maxLeaps = 64;

        public long initialBufferSegmentSize = 1_024 * 1_024;
        public long maxBufferSegmentSize = 1_024 * 1_024 * 1_024;

        public int numberOfDeltaStripes = 4;
        public int maxUpdatesBeforeDeltaStripeCompaction = 1_000_000;
        public int deltaStripeCompactionIntervalInMillis = 1_000 * 60;

        public int ackWatersStripingLevel = 1024;
        public int awaitOnlineStripingLevel = 1024;

        public boolean hardFsync = false;
        public long flushHighwatersAfterNUpdates = 10_000;

        public boolean useMemMap = false;

        public long takeCyaIntervalInMillis = 1_000;
        public long takeSlowThresholdInMillis = 1_000 * 60;
        public long takeLongPollTimeoutMillis = 10_000;
        public long takeSystemReofferDeltaMillis = 100;
        public long takeReofferDeltaMillis = 1_000;

        public long aquariumLeaderDeadAfterMillis = 60_000;
        public long aquariumLivelinessFeedEveryMillis = 500;
    }

    public interface IndexProviderRegistryCallback {

        void call(WALIndexProviderRegistry indexProviderRegistry, RowIOProvider<?> ephemeralRowIOProvider, RowIOProvider<?> persistentRowIOProvider);
    }

    public AmzaService initialize(AmzaServiceConfig config,
        AmzaStats amzaStats,
        SickThreads sickThreads,
        SickPartitions sickPartitions,
        BinaryPrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        RingMember ringMember,
        RingHost ringHost,
        TimestampedOrderIdProvider orderIdProvider,
        IdPacker idPacker,
        PartitionPropertyMarshaller partitionPropertyMarshaller,
        IndexProviderRegistryCallback indexProviderRegistryCallback,
        AvailableRowsTaker availableRowsTaker,
        RowsTakerFactory rowsTakerFactory,
        Optional<TakeFailureListener> takeFailureListener,
        RowChanges allRowChanges) throws Exception {

        AmzaPartitionWatcher amzaPartitionWatcher = new AmzaPartitionWatcher(allRowChanges);

        //TODO configure
        MemoryBackedRowIOProvider ephemeralRowIOProvider = new MemoryBackedRowIOProvider(config.workingDirectories,
            amzaStats.ioStats,
            config.corruptionParanoiaFactor,
            config.initialBufferSegmentSize,
            config.maxBufferSegmentSize,
            config.updatesBetweenLeaps,
            config.maxLeaps,
            new DirectByteBufferFactory());
        BinaryRowIOProvider persistentRowIOProvider = new BinaryRowIOProvider(config.workingDirectories,
            amzaStats.ioStats,
            config.corruptionParanoiaFactor,
            config.updatesBetweenLeaps,
            config.maxLeaps,
            config.useMemMap);
        WALIndexProviderRegistry indexProviderRegistry = new WALIndexProviderRegistry(ephemeralRowIOProvider, persistentRowIOProvider);
        indexProviderRegistryCallback.call(indexProviderRegistry, ephemeralRowIOProvider, persistentRowIOProvider);

        int tombstoneCompactionFactor = 2; // TODO expose to config;
        IndexedWALStorageProvider walStorageProvider = new IndexedWALStorageProvider(indexProviderRegistry, primaryRowMarshaller, highwaterRowMarshaller,
            orderIdProvider, sickPartitions, tombstoneCompactionFactor);
        PartitionIndex partitionIndex = new PartitionIndex(walStorageProvider, partitionPropertyMarshaller, config.hardFsync);

        TakeCoordinator takeCoordinator = new TakeCoordinator(amzaStats,
            orderIdProvider,
            idPacker,
            partitionIndex,
            config.takeCyaIntervalInMillis,
            config.takeSlowThresholdInMillis,
            config.takeSystemReofferDeltaMillis,
            config.takeReofferDeltaMillis);

        PartitionStore ringIndex = partitionIndex.get(PartitionCreator.RING_INDEX);
        PartitionStore nodeIndex = partitionIndex.get(PartitionCreator.NODE_INDEX);
        ConcurrentMap<IBA, CacheId<RingTopology>> ringsCache = new ConcurrentHashMap<>();
        ConcurrentMap<RingMember, CacheId<RingSet>> ringMemberRingNamesCache = new ConcurrentHashMap<>();
        AtomicLong nodeCacheId = new AtomicLong(0);
        AmzaRingStoreReader ringStoreReader = new AmzaRingStoreReader(ringMember, ringIndex, nodeIndex, ringsCache, ringMemberRingNamesCache, nodeCacheId);

        WALUpdated walUpdated = (versionedPartitionName, txId) -> {
            takeCoordinator.update(ringStoreReader, Preconditions.checkNotNull(versionedPartitionName), txId);
        };

        SystemWALStorage systemWALStorage = new SystemWALStorage(partitionIndex,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            amzaPartitionWatcher,
            config.hardFsync);

        PartitionCreator partitionCreator = new PartitionCreator(
            orderIdProvider,
            partitionPropertyMarshaller,
            partitionIndex,
            systemWALStorage,
            walUpdated,
            allRowChanges);

        final int deltaStorageStripes = config.numberOfDeltaStripes;
        PartitionStripeFunction partitionStripeFunction = new PartitionStripeFunction(deltaStorageStripes);

        File[] walDirs = new File[deltaStorageStripes];
        long[] stripeVersions = new long[deltaStorageStripes];
        for (int i = 0; i < deltaStorageStripes; i++) {
            walDirs[i] = new File(config.workingDirectories[i % config.workingDirectories.length], "delta-wal-" + i);
            if (!walDirs[i].exists()) {
                if (!walDirs[i].mkdirs()) {
                    throw new IllegalStateException("Please check your file permission. " + walDirs[i].getAbsolutePath());
                }
            }
            File versionFile = new File(walDirs[i], "version");
            if (versionFile.exists()) {
                try (FileInputStream fileInputStream = new FileInputStream(versionFile)) {
                    DataInput input = new DataInputStream(fileInputStream);
                    stripeVersions[i] = input.readLong();
                    LOG.info("Loaded stripeVersion:" + stripeVersions[i] + " for stripe:" + i + " from " + versionFile);
                }
            } else if (versionFile.createNewFile()) {
                try (FileOutputStream fileOutputStream = new FileOutputStream(versionFile)) {
                    DataOutput output = new DataOutputStream(fileOutputStream);
                    stripeVersions[i] = orderIdProvider.nextId();
                    output.writeLong(stripeVersions[i]);
                    LOG.info("Created stripeVersion:" + stripeVersions[i] + " for stripe:" + i + " to " + versionFile);
                }
            } else {
                throw new IllegalStateException("Please check your file permission. " + versionFile.getAbsolutePath());
            }
        }

        AwaitNotify<PartitionName> awaitOnline = new AwaitNotify<>(config.awaitOnlineStripingLevel);

        StorageVersionProvider storageVersionProvider = new StorageVersionProvider(orderIdProvider,
            ringMember,
            systemWALStorage,
            partitionIndex,
            ringStoreReader,
            partitionStripeFunction,
            stripeVersions,
            walUpdated,
            awaitOnline);

        long startupVersion = orderIdProvider.nextId();
        Member rootAquariumMember = ringMember.asAquariumMember();
        AmzaLivelinessStorage livelinessStorage = new AmzaLivelinessStorage(systemWALStorage, walUpdated, rootAquariumMember, startupVersion);
        AtQuorum livelinessAtQuorm = count -> count > ringStoreReader.getRingSize(AmzaRingReader.SYSTEM_RING) / 2;
        Liveliness liveliness = new Liveliness(System::currentTimeMillis,
            livelinessStorage,
            rootAquariumMember,
            livelinessAtQuorm,
            config.aquariumLeaderDeadAfterMillis,
            new AtomicLong(-1));

        AmzaAquariumProvider aquariumProvider = new AmzaAquariumProvider(startupVersion,
            ringMember,
            orderIdProvider,
            ringStoreReader,
            systemWALStorage,
            storageVersionProvider,
            partitionIndex,
            partitionCreator,
            takeCoordinator,
            walUpdated,
            liveliness,
            config.aquariumLivelinessFeedEveryMillis,
            awaitOnline);

        PartitionStateStorage partitionStateStorage = new PartitionStateStorage(ringMember,
            ringStoreReader,
            aquariumProvider,
            storageVersionProvider,
            takeCoordinator,
            awaitOnline);

        amzaPartitionWatcher.watch(PartitionCreator.PARTITION_VERSION_INDEX.getPartitionName(), storageVersionProvider);
        amzaPartitionWatcher.watch(PartitionCreator.AQUARIUM_STATE_INDEX.getPartitionName(), aquariumProvider);

        partitionIndex.open(partitionStateStorage, ringStoreReader);
        // cold start
        for (VersionedPartitionName versionedPartitionName : partitionIndex.getAllPartitions()) {
            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            partitionStateStorage.tx(versionedPartitionName.getPartitionName(), versionedAquarium -> {
                long partitionVersion = versionedAquarium.getVersionedPartitionName().getPartitionVersion();
                if (partitionVersion == versionedPartitionName.getPartitionVersion()) {
                    takeCoordinator.update(ringStoreReader, versionedPartitionName, partitionStore.highestTxId());
                } else {
                    LOG.warn("Version:{} wasn't aligned with versioned partition:{}", partitionVersion, versionedPartitionName);
                }
                return null;
            });
        }

        takeCoordinator.start(ringStoreReader, aquariumProvider);

        long maxUpdatesBeforeCompaction = config.maxUpdatesBeforeDeltaStripeCompaction;

        ExecutorService[] rowTakerThreadPools = new ExecutorService[deltaStorageStripes];
        RowsTaker[] rowsTakers = new RowsTaker[deltaStorageStripes];
        PartitionStripe[] partitionStripes = new PartitionStripe[deltaStorageStripes];
        HighwaterStorage[] highwaterStorages = new HighwaterStorage[deltaStorageStripes];
        for (int i = 0; i < deltaStorageStripes; i++) {
            rowTakerThreadPools[i] = Executors.newFixedThreadPool(config.numberOfTakerThreads,
                new ThreadFactoryBuilder().setNameFormat("stripe-" + i + "-rowTakerThreadPool-%d").build());

            rowsTakers[i] = rowsTakerFactory.create();

            RowIOProvider<File> ioProvider = new BinaryRowIOProvider(new String[] { walDirs[i].getAbsolutePath() },
                amzaStats.ioStats,
                config.corruptionParanoiaFactor,
                config.updatesBetweenLeaps,
                config.maxLeaps,
                config.useMemMap);
            DeltaWALFactory deltaWALFactory = new DeltaWALFactory(orderIdProvider, walDirs[i], ioProvider, primaryRowMarshaller, highwaterRowMarshaller);
            DeltaStripeWALStorage deltaWALStorage = new DeltaStripeWALStorage(
                i,
                amzaStats,
                sickThreads,
                deltaWALFactory,
                maxUpdatesBeforeCompaction);
            int stripeId = i;
            partitionStripes[i] = new PartitionStripe("stripe-" + i, partitionIndex, deltaWALStorage, partitionStateStorage, amzaPartitionWatcher,
                primaryRowMarshaller, highwaterRowMarshaller,
                (versionedPartitionName) -> {
                    if (!versionedPartitionName.getPartitionName().isSystemPartition()) {
                        return Math.abs(versionedPartitionName.getPartitionName().hashCode() % deltaStorageStripes) == stripeId;
                    }
                    return false;
                });
            highwaterStorages[i] = new PartitionBackedHighwaterStorage(orderIdProvider,
                ringMember,
                partitionIndex,
                systemWALStorage,
                walUpdated,
                config.flushHighwatersAfterNUpdates);
        }

        PartitionStripeProvider partitionStripeProvider = new PartitionStripeProvider(partitionStripeFunction,
            partitionStripes, highwaterStorages, rowTakerThreadPools, rowsTakers);

        HighestPartitionTx<Void> takeHighestPartitionTx = (versionedAquarium, highestTxId) -> {
            takeCoordinator.update(ringStoreReader, versionedAquarium.getVersionedPartitionName(), highestTxId);
            return null;
        };

        systemWALStorage.highestPartitionTxIds(takeHighestPartitionTx);

        ExecutorService stripeLoaderThreadPool = Executors.newFixedThreadPool(partitionStripes.length,
            new ThreadFactoryBuilder().setNameFormat("load-stripes-%d").build());
        List<Future> futures = new ArrayList<>();
        for (PartitionStripe partitionStripe : partitionStripes) {
            futures.add(stripeLoaderThreadPool.submit(() -> {
                try {
                    partitionStripe.load(partitionStateStorage);
                    partitionStripe.highestPartitionTxIds(takeHighestPartitionTx);
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

        ExecutorService compactDeltasThreadPool = Executors.newFixedThreadPool(config.numberOfDeltaStripes,
            new ThreadFactoryBuilder().setNameFormat("compact-deltas-%d").build());
        for (PartitionStripe partitionStripe : partitionStripes) {
            compactDeltasThreadPool.submit(() -> {
                while (true) {
                    try {
                        if (partitionStripe.mergeable()) {
                            partitionStripe.merge(false);
                        }
                        Object awakeCompactionLock = partitionStripe.getAwakeCompactionLock();
                        synchronized (awakeCompactionLock) {
                            awakeCompactionLock.wait(config.deltaStripeCompactionIntervalInMillis);
                        }

                    } catch (Throwable x) {
                        LOG.error("Compactor failed.", x);
                    }
                }
            });
        }

        AmzaRingStoreWriter amzaRingWriter = new AmzaRingStoreWriter(ringStoreReader,
            systemWALStorage,
            orderIdProvider,
            walUpdated,
            ringsCache,
            ringMemberRingNamesCache,
            nodeCacheId);
        amzaPartitionWatcher.watch(PartitionCreator.RING_INDEX.getPartitionName(), amzaRingWriter);
        amzaPartitionWatcher.watch(PartitionCreator.NODE_INDEX.getPartitionName(), amzaRingWriter);
        amzaRingWriter.register(ringMember, ringHost, -1);

        PartitionBackedHighwaterStorage systemHighwaterStorage = new PartitionBackedHighwaterStorage(orderIdProvider,
            ringMember,
            partitionIndex,
            systemWALStorage,
            walUpdated,
            config.flushHighwatersAfterNUpdates);
        RowChangeTaker changeTaker = new RowChangeTaker(amzaStats,
            ringStoreReader,
            ringHost,
            systemHighwaterStorage,
            rowsTakerFactory.create(),
            partitionIndex,
            partitionStripeProvider,
            partitionStripeFunction,
            partitionStateStorage,
            availableRowsTaker,
            new SystemPartitionCommitChanges(systemWALStorage, systemHighwaterStorage, walUpdated),
            new StripedPartitionCommitChanges(partitionStripeProvider, config.hardFsync, walUpdated),
            new OrderIdProviderImpl(new ConstantWriterIdProvider(1)),
            takeFailureListener,
            config.takeLongPollTimeoutMillis);

        PartitionTombstoneCompactor partitionCompactor = new PartitionTombstoneCompactor(amzaStats,
            partitionIndex,
            orderIdProvider,
            config.checkIfCompactionIsNeededIntervalInMillis,
            config.compactTombstoneIfOlderThanNMillis,
            config.numberOfCompactorThreads);

        PartitionComposter partitionComposter = new PartitionComposter(amzaStats, partitionIndex, partitionCreator, ringStoreReader, partitionStateStorage,
            partitionStripeProvider);

        AckWaters ackWaters = new AckWaters(config.ackWatersStripingLevel);

        return new AmzaService(orderIdProvider,
            amzaStats,
            ringStoreReader,
            amzaRingWriter,
            ackWaters,
            systemWALStorage,
            systemHighwaterStorage,
            takeCoordinator,
            partitionStateStorage,
            changeTaker,
            partitionCompactor,
            partitionComposter, // its all about being GREEN!!
            partitionIndex,
            partitionCreator,
            partitionStripeProvider,
            walUpdated,
            amzaPartitionWatcher,
            aquariumProvider,
            liveliness
        );
    }
}
