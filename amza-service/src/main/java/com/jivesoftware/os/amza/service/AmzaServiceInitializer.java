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
import com.jivesoftware.os.amza.api.partition.VersionedState;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.replication.AmzaAquariumProvider;
import com.jivesoftware.os.amza.service.replication.AmzaAquariumProvider.AmzaLivelinessStorage;
import com.jivesoftware.os.amza.service.replication.PartitionBackedHighwaterStorage;
import com.jivesoftware.os.amza.service.replication.PartitionComposter;
import com.jivesoftware.os.amza.service.replication.PartitionStateStorage;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.PartitionTombstoneCompactor;
import com.jivesoftware.os.amza.service.replication.RowChangeTaker;
import com.jivesoftware.os.amza.service.replication.StorageVersionProvider;
import com.jivesoftware.os.amza.service.replication.StripedPartitionCommitChanges;
import com.jivesoftware.os.amza.service.replication.SystemPartitionCommitChanges;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.service.storage.delta.DeltaStripeWALStorage;
import com.jivesoftware.os.amza.service.storage.delta.DeltaWALFactory;
import com.jivesoftware.os.amza.shared.AckWaters;
import com.jivesoftware.os.amza.shared.AwaitNotify;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.RowsTaker;
import com.jivesoftware.os.amza.shared.take.RowsTakerFactory;
import com.jivesoftware.os.amza.shared.take.TakeCoordinator;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class AmzaServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static class AmzaServiceConfig {

        public String[] workingDirectories = new String[]{"./var/data/"};

        public long checkIfCompactionIsNeededIntervalInMillis = 60_000;
        public long compactTombstoneIfOlderThanNMillis = 30 * 24 * 60 * 60 * 1000L;

        public int numberOfCompactorThreads = 8;
        public int numberOfTakerThreads = 8;

        public int corruptionParanoiaFactor = 10;

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

        public long aquariumLeaderDeadAfterMillis = 10_000;
        public long aquariumLivelinessFeedEveryMillis = 500;
    }

    public AmzaService initialize(AmzaServiceConfig config,
        AmzaStats amzaStats,
        BinaryPrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        RingMember ringMember,
        RingHost ringHost,
        TimestampedOrderIdProvider orderIdProvider,
        IdPacker idPacker,
        PartitionPropertyMarshaller partitionPropertyMarshaller,
        IndexedWALStorageProvider partitionsWALStorageProvider,
        AvailableRowsTaker availableRowsTaker,
        RowsTakerFactory rowsTakerFactory,
        Optional<TakeFailureListener> takeFailureListener,
        RowChanges allRowChanges) throws Exception {

        AmzaPartitionWatcher amzaPartitionWatcher = new AmzaPartitionWatcher(allRowChanges);

        RowIOProvider ioProvider = new BinaryRowIOProvider(amzaStats.ioStats, config.corruptionParanoiaFactor, config.useMemMap);

        PartitionIndex partitionIndex = new PartitionIndex(config.workingDirectories, "amza/stores",
            partitionsWALStorageProvider, partitionPropertyMarshaller, config.hardFsync);

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
        ConcurrentMap<IBA, Integer> ringSizesCache = new ConcurrentHashMap<>();
        ConcurrentMap<RingMember, Set<IBA>> ringMemberRingNamesCache = new ConcurrentHashMap<>();
        AmzaRingStoreReader amzaRingReader = new AmzaRingStoreReader(ringMember, ringIndex, nodeIndex, ringSizesCache, ringMemberRingNamesCache);

        WALUpdated walUpdated = (versionedPartitionName, waterline, isOnline, txId) -> {
            takeCoordinator.updated(amzaRingReader, Preconditions.checkNotNull(versionedPartitionName), waterline, isOnline, txId);
        };

        SystemWALStorage systemWALStorage = new SystemWALStorage(partitionIndex,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            amzaPartitionWatcher,
            config.hardFsync);

        final int deltaStorageStripes = config.numberOfDeltaStripes;
        PartitionStripeProvider.PartitionStripeFunction stripeFunction = partitionName -> Math.abs(partitionName.hashCode() % deltaStorageStripes);

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
            } else {
                if (versionFile.createNewFile()) {
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
        }

        AwaitNotify<PartitionName> awaitOnline = new AwaitNotify<>(config.awaitOnlineStripingLevel);

        StorageVersionProvider storageVersionProvider = new StorageVersionProvider(orderIdProvider,
            ringMember,
            systemWALStorage,
            stripeFunction,
            stripeVersions,
            walUpdated,
            awaitOnline);

        long startupVersion = orderIdProvider.nextId();
        Member rootAquariumMember = ringMember.asAquariumMember();
        AmzaLivelinessStorage livelinessStorage = new AmzaLivelinessStorage(systemWALStorage, walUpdated, rootAquariumMember, startupVersion);
        AtQuorum livelinessAtQuorm = count -> count > amzaRingReader.getRingSize(AmzaRingReader.SYSTEM_RING) / 2;
        Liveliness liveliness = new Liveliness(System::currentTimeMillis,
            livelinessStorage,
            rootAquariumMember,
            livelinessAtQuorm,
            config.aquariumLeaderDeadAfterMillis,
            new AtomicLong(-1));

        AmzaAquariumProvider aquariumProvider = new AmzaAquariumProvider(startupVersion,
            ringMember,
            orderIdProvider,
            amzaRingReader,
            systemWALStorage,
            storageVersionProvider,
            partitionIndex,
            walUpdated,
            liveliness,
            awaitOnline);

        PartitionStateStorage partitionStateStorage = new PartitionStateStorage(ringMember,
            aquariumProvider,
            storageVersionProvider,
            takeCoordinator,
            awaitOnline);

        amzaPartitionWatcher.watch(PartitionCreator.PARTITION_VERSION_INDEX.getPartitionName(), storageVersionProvider);
        amzaPartitionWatcher.watch(PartitionCreator.AQUARIUM_STATE_INDEX.getPartitionName(), aquariumProvider);

        partitionIndex.open(partitionStateStorage);
        // cold start
        for (VersionedPartitionName versionedPartitionName : partitionIndex.getAllPartitions()) {
            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            VersionedState state = partitionStateStorage.getLocalVersionedState(versionedPartitionName.getPartitionName());
            if (state != null && state.storageVersion.partitionVersion == versionedPartitionName.getPartitionVersion()) {
                takeCoordinator.updated(amzaRingReader, versionedPartitionName, state.waterline, state.isOnline, partitionStore.highestTxId());
            } else {
                LOG.warn("State:{} wasn't aligned with versioned partition:{}", state, versionedPartitionName);
            }
        }

        takeCoordinator.start(amzaRingReader);

        long maxUpdatesBeforeCompaction = config.maxUpdatesBeforeDeltaStripeCompaction;

        ExecutorService[] rowTakerThreadPools = new ExecutorService[deltaStorageStripes];
        RowsTaker[] rowsTakers = new RowsTaker[deltaStorageStripes];
        PartitionStripe[] partitionStripes = new PartitionStripe[deltaStorageStripes];
        HighwaterStorage[] highwaterStorages = new HighwaterStorage[deltaStorageStripes];
        for (int i = 0; i < deltaStorageStripes; i++) {
            rowTakerThreadPools[i] = Executors.newFixedThreadPool(config.numberOfTakerThreads,
                new ThreadFactoryBuilder().setNameFormat("stripe-" + i + "-rowTakerThreadPool-%d").build());

            rowsTakers[i] = rowsTakerFactory.create();

            DeltaWALFactory deltaWALFactory = new DeltaWALFactory(orderIdProvider, walDirs[i], ioProvider, primaryRowMarshaller, highwaterRowMarshaller);
            DeltaStripeWALStorage deltaWALStorage = new DeltaStripeWALStorage(
                i,
                amzaStats,
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
            highwaterStorages[i] = new PartitionBackedHighwaterStorage(orderIdProvider, ringMember, systemWALStorage, walUpdated,
                config.flushHighwatersAfterNUpdates);
        }

        PartitionStripeProvider partitionStripeProvider = new PartitionStripeProvider(stripeFunction,
            partitionStripes, highwaterStorages, rowTakerThreadPools, rowsTakers);

        PartitionCreator partitionProvider = new PartitionCreator(
            orderIdProvider,
            partitionPropertyMarshaller,
            partitionIndex,
            systemWALStorage,
            walUpdated,
            allRowChanges);

        HighestPartitionTx<Void> takeHighestPartitionTx = (versionedPartitionName, partitionState, isOnline, highestTxId) -> {
            takeCoordinator.updated(amzaRingReader, versionedPartitionName, partitionState, isOnline, highestTxId);
            return null;
        };

        systemWALStorage.highestPartitionTxIds(takeHighestPartitionTx);

        ExecutorService stripeLoaderThreadPool = Executors.newFixedThreadPool(partitionStripes.length,
            new ThreadFactoryBuilder().setNameFormat("load-stripes-%d").build());
        List<Future> futures = new ArrayList<>();
        for (PartitionStripe partitionStripe : partitionStripes) {
            futures.add(stripeLoaderThreadPool.submit(() -> {
                try {
                    partitionStripe.load();
                    partitionStripe.highestPartitionTxIds(takeHighestPartitionTx);
                } catch (Exception x) {
                    LOG.error("Failed while loading " + partitionStripe, x);
                    throw new RuntimeException(x);
                }
            }));
        }
        for (Future future : futures) {
            future.get();
        }
        stripeLoaderThreadPool.shutdown();

        ExecutorService compactDeltasThreadPool = Executors.newFixedThreadPool(config.numberOfDeltaStripes);
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

        AmzaRingStoreWriter amzaRingWriter = new AmzaRingStoreWriter(amzaRingReader,
            systemWALStorage,
            orderIdProvider,
            walUpdated,
            ringSizesCache,
            ringMemberRingNamesCache);
        amzaPartitionWatcher.watch(PartitionCreator.RING_INDEX.getPartitionName(), amzaRingWriter);
        amzaRingWriter.register(ringMember, ringHost);
        amzaRingWriter.addRingMember(AmzaRingReader.SYSTEM_RING, ringMember);

        PartitionBackedHighwaterStorage systemHighwaterStorage = new PartitionBackedHighwaterStorage(orderIdProvider, ringMember, systemWALStorage, walUpdated,
            config.flushHighwatersAfterNUpdates);
        RowChangeTaker changeTaker = new RowChangeTaker(amzaStats,
            amzaRingReader,
            ringHost,
            systemHighwaterStorage,
            partitionIndex,
            partitionStripeProvider,
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

        PartitionComposter partitionComposter = new PartitionComposter(amzaStats, partitionIndex, partitionProvider, amzaRingReader, partitionStateStorage,
            partitionStripeProvider);

        AckWaters ackWaters = new AckWaters(config.ackWatersStripingLevel);

        // last minute initialization
        aquariumProvider.start(config.aquariumLivelinessFeedEveryMillis);

        return new AmzaService(orderIdProvider,
            amzaStats,
            amzaRingReader,
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
            partitionProvider,
            partitionStripeProvider,
            walUpdated,
            amzaPartitionWatcher,
            aquariumProvider,
            liveliness
        );
    }
}
