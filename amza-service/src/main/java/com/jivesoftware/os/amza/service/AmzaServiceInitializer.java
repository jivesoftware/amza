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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.replication.PartitionBackedHighwaterStorage;
import com.jivesoftware.os.amza.service.replication.PartitionChangeTaker;
import com.jivesoftware.os.amza.service.replication.PartitionCompactor;
import com.jivesoftware.os.amza.service.replication.PartitionComposter;
import com.jivesoftware.os.amza.service.replication.PartitionStatusStorage;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.service.storage.SystemStripeWALStorage;
import com.jivesoftware.os.amza.service.storage.delta.DeltaStripeWALStorage;
import com.jivesoftware.os.amza.service.storage.delta.DeltaWALFactory;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionTx;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker;
import com.jivesoftware.os.amza.shared.wal.WALStorageProvider;
import com.jivesoftware.os.amza.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AmzaServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static class AmzaServiceConfig {

        public String[] workingDirectories = new String[] { "./var/data/" };

        public int resendReplicasIntervalInMillis = 1000;
        public int applyReplicasIntervalInMillis = 1000;
        public int takeFromNeighborsIntervalInMillis = 1000;

        public long checkIfCompactionIsNeededIntervalInMillis = 60_000;
        public long compactTombstoneIfOlderThanNMillis = 30 * 24 * 60 * 60 * 1000L;

        public int numberOfResendThreads = 8;
        public int numberOfApplierThreads = 8;
        public int numberOfCompactorThreads = 8;
        public int numberOfTakerThreads = 8;
        public int numberOfReplicatorThreads = 24;

        public int corruptionParanoiaFactor = 10;

        public int numberOfDeltaStripes = 4;
        public int maxUpdatesBeforeDeltaStripeCompaction = 1_000_000;
        public int deltaStripeCompactionIntervalInMillis = 1_000 * 60;

        public boolean hardFsync = false;

        public boolean useMemMap = false;
    }

    public AmzaService initialize(AmzaServiceConfig config,
        AmzaStats amzaStats,
        BinaryPrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        RingMember ringMember,
        RingHost ringHost,
        TimestampedOrderIdProvider orderIdProvider,
        PartitionPropertyMarshaller partitionPropertyMarshaller,
        WALStorageProvider partitionsWALStorageProvider,
        UpdatesTaker updatesTaker,
        Optional<SendFailureListener> sendFailureListener,
        Optional<TakeFailureListener> takeFailureListener,
        RowChanges allRowChanges) throws Exception {

        AmzaPartitionWatcher amzaPartitionWatcher = new AmzaPartitionWatcher(allRowChanges);

        RowIOProvider ioProvider = new BinaryRowIOProvider(amzaStats.ioStats, config.corruptionParanoiaFactor, config.useMemMap);

        PartitionIndex partitionIndex = new PartitionIndex(amzaStats, config.workingDirectories, "amza/stores",
            partitionsWALStorageProvider, partitionPropertyMarshaller, config.hardFsync);

        AmzaRingReader amzaRingReader = new AmzaRingReader(ringMember, partitionIndex);

        PartitionStripe systemPartitionStripe = new PartitionStripe("system",
            amzaStats,
            partitionIndex,
            new SystemStripeWALStorage(),
            new TxPartitionStatus() {

                @Override
                public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
                    return tx.tx(new VersionedPartitionName(partitionName, 0), TxPartitionStatus.Status.ONLINE);
                }
            },
            amzaPartitionWatcher,
            (VersionedPartitionName input) -> input.getPartitionName().isSystemPartition());

        PartitionStatusStorage partitionStatusStorage = new PartitionStatusStorage(orderIdProvider, ringMember, systemPartitionStripe);
        partitionIndex.open(partitionStatusStorage);

        final int deltaStorageStripes = config.numberOfDeltaStripes;
        long maxUpdatesBeforeCompaction = config.maxUpdatesBeforeDeltaStripeCompaction;

        PartitionBackedHighwaterStorage highwaterStorage = new PartitionBackedHighwaterStorage(orderIdProvider, ringMember, systemPartitionStripe);

        PartitionStripe[] partitionStripes = new PartitionStripe[deltaStorageStripes];
        for (int i = 0; i < deltaStorageStripes; i++) {
            File walDir = new File(config.workingDirectories[i % config.workingDirectories.length], "delta-wal-" + i);
            DeltaWALFactory deltaWALFactory = new DeltaWALFactory(orderIdProvider, walDir, ioProvider, primaryRowMarshaller, highwaterRowMarshaller, -1);
            DeltaStripeWALStorage deltaWALStorage = new DeltaStripeWALStorage(highwaterStorage,
                i,
                primaryRowMarshaller,
                highwaterRowMarshaller,
                deltaWALFactory,
                maxUpdatesBeforeCompaction);
            int stripeId = i;
            partitionStripes[i] = new PartitionStripe("stripe-" + i, amzaStats, partitionIndex, deltaWALStorage, partitionStatusStorage, amzaPartitionWatcher,
                (versionedPartitionName) -> {
                    if (!versionedPartitionName.getPartitionName().isSystemPartition()) {
                        return Math.abs(versionedPartitionName.getPartitionName().hashCode()) % deltaStorageStripes == stripeId;
                    }
                    return false;
                });
        }

        PartitionStripeProvider partitionStripeProvider = new PartitionStripeProvider(systemPartitionStripe, partitionStripes);

        PartitionProvider partitionProvider = new PartitionProvider(
            orderIdProvider,
            partitionPropertyMarshaller,
            partitionIndex,
            allRowChanges,
            config.hardFsync);

        ExecutorService stripeLoaderThreadPool = Executors.newFixedThreadPool(partitionStripes.length,
            new ThreadFactoryBuilder().setNameFormat("load-stripes-%d").build());
        List<Future> futures = new ArrayList<>();
        for (final PartitionStripe partitionStripe : partitionStripes) {
            futures.add(stripeLoaderThreadPool.submit(() -> {
                try {
                    partitionStripe.load();
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

        ScheduledExecutorService compactDeltasThreadPool = Executors.newScheduledThreadPool(config.numberOfCompactorThreads,
            new ThreadFactoryBuilder().setNameFormat("compact-deltas-%d").build());
        for (final PartitionStripe partitionStripe : partitionStripes) {
            compactDeltasThreadPool.scheduleAtFixedRate(() -> {
                try {
                    partitionStripe.compact();
                } catch (Throwable x) {
                    LOG.error("Compactor failed.", x);
                }
            }, config.deltaStripeCompactionIntervalInMillis, config.deltaStripeCompactionIntervalInMillis, TimeUnit.MILLISECONDS);
        }

        AmzaHostRing amzaHostRing = new AmzaHostRing(amzaRingReader, systemPartitionStripe, orderIdProvider);
        amzaPartitionWatcher.watch(PartitionProvider.RING_INDEX.getPartitionName(), amzaHostRing);
        amzaHostRing.register(ringMember, ringHost);
        amzaHostRing.addRingMember("system", ringMember);

        PartitionChangeTaker changeTaker = new PartitionChangeTaker(amzaStats,
            amzaRingReader,
            ringHost,
            partitionIndex,
            partitionStripeProvider,
            partitionStripes,
            highwaterStorage,
            partitionStatusStorage,
            updatesTaker,
            takeFailureListener,
            config.takeFromNeighborsIntervalInMillis,
            config.numberOfTakerThreads,
            config.hardFsync);

        PartitionCompactor partitionCompactor = new PartitionCompactor(amzaStats,
            partitionIndex,
            orderIdProvider,
            config.checkIfCompactionIsNeededIntervalInMillis,
            config.compactTombstoneIfOlderThanNMillis,
            config.numberOfCompactorThreads);

        PartitionComposter partitionComposter = new PartitionComposter(partitionIndex, partitionProvider, amzaRingReader, partitionStatusStorage,
            partitionStripeProvider);

        RecentPartitionTakers recentPartitionTakers = new RecentPartitionTakers();

        return new AmzaService(orderIdProvider,
            amzaStats,
            amzaRingReader,
            amzaHostRing,
            highwaterStorage,
            partitionStatusStorage,
            changeTaker,
            partitionCompactor,
            partitionComposter, // its all about being GREEN!!
            partitionIndex,
            partitionProvider,
            partitionStripeProvider,
            recentPartitionTakers,
            amzaPartitionWatcher);
    }
}
