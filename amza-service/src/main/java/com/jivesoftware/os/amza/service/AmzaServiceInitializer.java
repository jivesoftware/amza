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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.BAInterner;
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
import com.jivesoftware.os.amza.service.take.RowsTakerFactory;
import com.jivesoftware.os.amza.service.take.TakeCoordinator;
import com.jivesoftware.os.aquarium.Liveliness;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.aquarium.interfaces.AtQuorum;
import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AmzaServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static class AmzaServiceConfig {

        public String[] workingDirectories = null;

        public long asyncFsyncIntervalMillis = 1_000;

        public int numberOfTakerThreads = 8;

        public int corruptionParanoiaFactor = 10;
        public int updatesBetweenLeaps = 4_096;
        public int maxLeaps = 64;

        public long initialBufferSegmentSize = 1_024 * 1_024;
        public long maxBufferSegmentSize = 1_024 * 1_024 * 1_024;

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

        public int tombstoneCompactionFactor = 2;
        public long checkIfCompactionIsNeededIntervalInMillis = 60_000;
        public long rebalanceableEveryNMillis = TimeUnit.HOURS.toMillis(1);
        public long rebalanceIfImbalanceGreaterThanNBytes = 1024 * 1024 * 1024;

        public long interruptBlockingReadsIfLingersForNMillis = 60_000;

        public boolean rackDistributionEnabled = true;
    }

    public interface IndexProviderRegistryCallback {

        void call(File[] workingIndexDirectories,
            WALIndexProviderRegistry indexProviderRegistry,
            RowIOProvider ephemeralRowIOProvider,
            RowIOProvider persistentRowIOProvider,
            int numberOfStripes) throws Exception;
    }

    public AmzaService initialize(AmzaServiceConfig config,
        BAInterner interner,
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

        AmzaPartitionWatcher amzaSystemPartitionWatcher = new AmzaPartitionWatcher(true, allRowChanges);

        int numberOfStripes = config.workingDirectories.length;

        //TODO configure
        MemoryBackedRowIOProvider ephemeralRowIOProvider = new MemoryBackedRowIOProvider(
            amzaStats.ioStats,
            config.initialBufferSegmentSize,
            config.maxBufferSegmentSize,
            config.updatesBetweenLeaps,
            config.maxLeaps,
            new DirectByteBufferFactory());

        BinaryRowIOProvider persistentRowIOProvider = new BinaryRowIOProvider(
            amzaStats.ioStats,
            config.updatesBetweenLeaps,
            config.maxLeaps,
            config.useMemMap);

        File[] workingWALDirectories = new File[config.workingDirectories.length];
        File[] workingIndexDirectories = new File[config.workingDirectories.length];
        for (int i = 0; i < workingWALDirectories.length; i++) {
            workingWALDirectories[i] = new File(config.workingDirectories[i], "wal");
            workingIndexDirectories[i] = new File(config.workingDirectories[i], "index");
        }

        WALIndexProviderRegistry indexProviderRegistry = new WALIndexProviderRegistry(ephemeralRowIOProvider, persistentRowIOProvider);
        indexProviderRegistryCallback.call(workingIndexDirectories, indexProviderRegistry, ephemeralRowIOProvider, persistentRowIOProvider, numberOfStripes);

        IndexedWALStorageProvider walStorageProvider = new IndexedWALStorageProvider(amzaStats,
            workingWALDirectories,
            numberOfStripes,
            indexProviderRegistry,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            orderIdProvider,
            sickPartitions,
            config.tombstoneCompactionFactor,
            config.rebalanceIfImbalanceGreaterThanNBytes);

        int numProc = Runtime.getRuntime().availableProcessors();

        PartitionIndex partitionIndex = new PartitionIndex(amzaStats, orderIdProvider, walStorageProvider, numProc);

        SystemWALStorage systemWALStorage = new SystemWALStorage(partitionIndex,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            amzaSystemPartitionWatcher,
            config.hardFsync);

        File[] walDirs = new File[numberOfStripes];
        long[] stripeVersions = new long[numberOfStripes];
        for (int i = 0; i < numberOfStripes; i++) {
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

        ConcurrentBAHash<CacheId<RingTopology>> ringsCache = new ConcurrentBAHash<>(13, true, numProc);
        ConcurrentBAHash<CacheId<RingSet>> ringMemberRingNamesCache = new ConcurrentBAHash<>(13, true, numProc);

        AtomicLong nodeCacheId = new AtomicLong(0);
        AmzaRingStoreReader ringStoreReader = new AmzaRingStoreReader(interner,
            ringMember,
            ringsCache,
            ringMemberRingNamesCache,
            nodeCacheId);

        AwaitNotify<PartitionName> awaitOnline = new AwaitNotify<>(config.awaitOnlineStripingLevel);

        long maxUpdatesBeforeCompaction = config.maxUpdatesBeforeDeltaStripeCompaction;

        AckWaters ackWaters = new AckWaters(amzaStats, config.ackWatersStripingLevel);

        DeltaStripeWALStorage[] deltaStripeWALStorages = new DeltaStripeWALStorage[numberOfStripes];

        BinaryRowIOProvider deltaRowIOProvider = new BinaryRowIOProvider(
            amzaStats.ioStats,
            -1,
            0,
            config.useMemMap);
        for (int i = 0; i < numberOfStripes; i++) {

            DeltaWALFactory deltaWALFactory = new DeltaWALFactory(orderIdProvider, walDirs[i], deltaRowIOProvider, primaryRowMarshaller,
                highwaterRowMarshaller, config.corruptionParanoiaFactor);

            deltaStripeWALStorages[i] = new DeltaStripeWALStorage(
                interner,
                i,
                amzaStats,
                ackWaters,
                sickThreads,
                ringStoreReader,
                deltaWALFactory,
                indexProviderRegistry,
                maxUpdatesBeforeCompaction,
                numProc);
        }

        List<WALUpdated> walUpdateDelegates = Lists.newCopyOnWriteArrayList();
        WALUpdated walUpdated = (versionedPartitionName, txId) -> {
            for (WALUpdated delegate : walUpdateDelegates) {
                delegate.updated(versionedPartitionName, txId);
            }
        };

        PartitionCreator partitionCreator = new PartitionCreator(
            orderIdProvider,
            partitionPropertyMarshaller,
            partitionIndex,
            systemWALStorage,
            walUpdated,
            allRowChanges,
            interner);

        StorageVersionProvider storageVersionProvider = new StorageVersionProvider(interner,
            orderIdProvider,
            ringMember,
            systemWALStorage,
            partitionCreator,
            ringStoreReader,
            workingIndexDirectories,
            stripeVersions,
            deltaStripeWALStorages,
            walUpdated,
            awaitOnline);

        amzaSystemPartitionWatcher.watch(PartitionCreator.PARTITION_VERSION_INDEX.getPartitionName(), storageVersionProvider);
        amzaSystemPartitionWatcher.watch(PartitionCreator.REGION_PROPERTIES.getPartitionName(), partitionCreator);

        TakeCoordinator takeCoordinator = new TakeCoordinator(systemWALStorage,
            ringMember,
            amzaStats,
            orderIdProvider,
            idPacker,
            partitionCreator,
            config.takeCyaIntervalInMillis,
            config.takeSlowThresholdInMillis,
            config.takeSystemReofferDeltaMillis,
            config.takeReofferDeltaMillis);
        walUpdateDelegates.add((versionedPartitionName, txId) -> {
            takeCoordinator.update(ringStoreReader, Preconditions.checkNotNull(versionedPartitionName), txId);
        });

        long startupVersion = orderIdProvider.nextId();
        Member rootAquariumMember = ringMember.asAquariumMember();
        AmzaLivelinessStorage livelinessStorage = new AmzaLivelinessStorage(systemWALStorage, orderIdProvider, walUpdated, rootAquariumMember, startupVersion);
        AtQuorum livelinessAtQuorm = count -> count > ringStoreReader.getRingSize(AmzaRingReader.SYSTEM_RING) / 2;
        Liveliness liveliness = new Liveliness(System::currentTimeMillis,
            livelinessStorage,
            rootAquariumMember,
            livelinessAtQuorm,
            config.aquariumLeaderDeadAfterMillis,
            new AtomicLong(-1));

        AmzaAquariumProvider aquariumProvider = new AmzaAquariumProvider(interner,
            ringMember,
            orderIdProvider,
            ringStoreReader,
            systemWALStorage,
            storageVersionProvider,
            partitionCreator,
            takeCoordinator,
            walUpdated,
            liveliness,
            config.aquariumLivelinessFeedEveryMillis,
            awaitOnline, sickThreads);
        amzaSystemPartitionWatcher.watch(PartitionCreator.AQUARIUM_STATE_INDEX.getPartitionName(), aquariumProvider);

        AmzaPartitionWatcher amzaStripedPartitionWatcher = new AmzaPartitionWatcher(false, allRowChanges);

        HighwaterStorage highwaterStorage = new PartitionBackedHighwaterStorage(interner,
            orderIdProvider,
            ringMember,
            partitionCreator,
            systemWALStorage,
            walUpdated,
            config.flushHighwatersAfterNUpdates);

        PartitionStripeProvider partitionStripeProvider = new PartitionStripeProvider(
            amzaStats,
            partitionCreator,
            partitionIndex,
            primaryRowMarshaller,
            deltaStripeWALStorages,
            highwaterStorage,
            highwaterRowMarshaller,
            ringMember,
            ringStoreReader,
            aquariumProvider,
            storageVersionProvider,
            takeCoordinator,
            awaitOnline,
            amzaStripedPartitionWatcher,
            config.asyncFsyncIntervalMillis,
            config.deltaStripeCompactionIntervalInMillis);

        PartitionComposter partitionComposter = new PartitionComposter(amzaStats, partitionIndex, partitionCreator, ringStoreReader,
            partitionStripeProvider, storageVersionProvider, interner, numProc);
        amzaSystemPartitionWatcher.watch(PartitionCreator.REGION_INDEX.getPartitionName(), partitionComposter);
        amzaSystemPartitionWatcher.watch(PartitionCreator.PARTITION_VERSION_INDEX.getPartitionName(), partitionComposter);
        amzaSystemPartitionWatcher.watch(PartitionCreator.AQUARIUM_STATE_INDEX.getPartitionName(), partitionComposter);

        AmzaRingStoreWriter amzaRingWriter = new AmzaRingStoreWriter(ringStoreReader,
            systemWALStorage,
            orderIdProvider,
            walUpdated,
            ringsCache,
            ringMemberRingNamesCache,
            nodeCacheId,
            config.rackDistributionEnabled);
        amzaSystemPartitionWatcher.watch(PartitionCreator.RING_INDEX.getPartitionName(), amzaRingWriter);
        amzaSystemPartitionWatcher.watch(PartitionCreator.NODE_INDEX.getPartitionName(), amzaRingWriter);

        AmzaSystemReady systemReady = new AmzaSystemReady(ringStoreReader, partitionCreator, sickPartitions, sickThreads);
        systemReady.onReady(() -> {
            LOG.info("Loading highest txIds after system ready...");
            int count = 0;
            for (PartitionName partitionName : partitionCreator.getMemberPartitions(ringStoreReader)) {
                count++;
                try {
                    partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage1, versionedAquarium) -> {
                        return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                            VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
                            PartitionStore partitionStore = partitionCreator.get(versionedPartitionName, stripeIndex);
                            if (partitionStore != null) {
                                takeCoordinator.update(ringStoreReader, versionedPartitionName, partitionStore.highestTxId());
                            } else {
                                LOG.warn("Skipped system ready init for a partition, likely because it is only partially defined: {}", versionedPartitionName);
                            }
                            return null;
                        });
                    });
                } catch (PartitionIsDisposedException x) {
                    LOG.info("Skipped a partition because its disposed: {}", partitionName);
                } catch (PropertiesNotPresentException x) {
                    LOG.warn("Skipped system ready init for a partition because its properties were missing: {}", partitionName);
                } catch (Exception x) {
                    LOG.error("Failed system ready init for a partition, please fix: {}", new Object[]{partitionName}, x);
                }
            }
            LOG.info("Finished loading {} highest txIds after system ready!", count);
            return null;
        });

        RowChangeTaker changeTaker = new RowChangeTaker(amzaStats,
            numberOfStripes,
            storageVersionProvider,
            ringStoreReader,
            systemReady,
            ringHost,
            rowsTakerFactory.create(),
            partitionStripeProvider,
            availableRowsTaker,
            Executors.newFixedThreadPool(config.numberOfTakerThreads, new ThreadFactoryBuilder().setNameFormat("rowTakerThreadPool-%d").build()),
            rowsTakerFactory.create(),
            new SystemPartitionCommitChanges(storageVersionProvider, systemWALStorage, highwaterStorage, walUpdated),
            new StripedPartitionCommitChanges(partitionStripeProvider, config.hardFsync, walUpdated),
            new OrderIdProviderImpl(new ConstantWriterIdProvider(1)),
            takeFailureListener,
            config.takeLongPollTimeoutMillis,
            primaryRowMarshaller,
            highwaterRowMarshaller);

        PartitionTombstoneCompactor partitionCompactor = new PartitionTombstoneCompactor(walStorageProvider,
            partitionCreator,
            partitionIndex,
            storageVersionProvider,
            config.checkIfCompactionIsNeededIntervalInMillis,
            config.rebalanceableEveryNMillis,
            numberOfStripes);

        return new AmzaService(orderIdProvider,
            amzaStats,
            numberOfStripes,
            storageVersionProvider,
            ringStoreReader,
            amzaRingWriter,
            ackWaters,
            systemWALStorage,
            highwaterStorage,
            takeCoordinator,
            changeTaker,
            partitionCompactor,
            partitionComposter, // its all about being GREEN!!
            partitionIndex,
            partitionCreator,
            partitionStripeProvider,
            walUpdated,
            amzaSystemPartitionWatcher,
            amzaStripedPartitionWatcher,
            aquariumProvider,
            systemReady,
            liveliness);
    }
}
