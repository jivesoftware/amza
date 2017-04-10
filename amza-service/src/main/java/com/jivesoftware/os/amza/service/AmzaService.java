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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.RemoteVersionedState;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.replication.AmzaAquariumProvider;
import com.jivesoftware.os.amza.service.replication.PartitionComposter;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.PartitionTombstoneCompactor;
import com.jivesoftware.os.amza.service.replication.RowChangeTaker;
import com.jivesoftware.os.amza.service.replication.StorageVersionProvider;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.service.take.TakeCoordinator;
import com.jivesoftware.os.aquarium.Liveliness;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang.mutable.MutableLong;
import org.xerial.snappy.SnappyOutputStream;

/**
 * Amza pronounced (AH m z ah )
 * Sanskrit word meaning partition / share.
 */
public class AmzaService implements AmzaInstance, PartitionProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TimestampedOrderIdProvider orderIdProvider;
    private final AmzaStats amzaStats;
    private final int numberOfStripes;
    private final WALIndexProviderRegistry indexProviderRegistry;
    private final StorageVersionProvider storageVersionProvider;
    private final AmzaRingStoreReader ringStoreReader;
    private final AmzaRingStoreWriter ringStoreWriter;
    private final AckWaters ackWaters;
    private final SystemWALStorage systemWALStorage;
    private final HighwaterStorage highwaterStorage;
    private final TakeCoordinator takeCoordinator;
    private final RowChangeTaker changeTaker;
    private final PartitionTombstoneCompactor partitionTombstoneCompactor;
    private final PartitionComposter partitionComposter;
    private final PartitionIndex partitionIndex;
    private final PartitionCreator partitionCreator;
    private final PartitionStripeProvider partitionStripeProvider;
    private final WALUpdated walUpdated;
    private final AmzaPartitionWatcher amzaSystemPartitionWatcher;
    private final AmzaPartitionWatcher amzaStripedPartitionWatcher;
    private final AmzaAquariumProvider aquariumProvider;
    private final TakeFullySystemReady systemReady;
    private final Liveliness liveliness;

    public AmzaService(TimestampedOrderIdProvider orderIdProvider,
        AmzaStats amzaStats,
        int numberOfStripes,
        WALIndexProviderRegistry indexProviderRegistry,
        StorageVersionProvider storageVersionProvider,
        AmzaRingStoreReader ringReader,
        AmzaRingStoreWriter amzaHostRing,
        AckWaters ackWaters,
        SystemWALStorage systemWALStorage,
        HighwaterStorage highwaterStorage,
        TakeCoordinator takeCoordinator,
        RowChangeTaker changeTaker,
        PartitionTombstoneCompactor partitionTombstoneCompactor,
        PartitionComposter partitionComposter,
        PartitionIndex partitionIndex,
        PartitionCreator partitionCreator,
        PartitionStripeProvider partitionStripeProvider,
        WALUpdated walUpdated,
        AmzaPartitionWatcher amzaSystemPartitionWatcher,
        AmzaPartitionWatcher amzaStripedPartitionWatcher,
        AmzaAquariumProvider aquariumProvider,
        TakeFullySystemReady systemReady,
        Liveliness liveliness) {

        this.amzaStats = amzaStats;
        this.numberOfStripes = numberOfStripes;
        this.indexProviderRegistry = indexProviderRegistry;
        this.storageVersionProvider = storageVersionProvider;
        this.orderIdProvider = orderIdProvider;
        this.ringStoreReader = ringReader;
        this.ringStoreWriter = amzaHostRing;
        this.ackWaters = ackWaters;
        this.systemWALStorage = systemWALStorage;
        this.highwaterStorage = highwaterStorage;
        this.takeCoordinator = takeCoordinator;
        this.changeTaker = changeTaker;
        this.partitionTombstoneCompactor = partitionTombstoneCompactor;
        this.partitionComposter = partitionComposter;
        this.partitionIndex = partitionIndex;
        this.partitionCreator = partitionCreator;
        this.partitionStripeProvider = partitionStripeProvider;
        this.walUpdated = walUpdated;
        this.amzaSystemPartitionWatcher = amzaSystemPartitionWatcher;
        this.amzaStripedPartitionWatcher = amzaStripedPartitionWatcher;
        this.aquariumProvider = aquariumProvider;
        this.systemReady = systemReady;
        this.liveliness = liveliness;
    }

    public PartitionIndex getPartitionIndex() {
        return partitionIndex;
    }

    public AmzaRingStoreReader getRingReader() {
        return ringStoreReader;
    }

    public AmzaRingStoreWriter getRingWriter() {
        return ringStoreWriter;
    }

    public PartitionComposter getPartitionComposter() {
        return partitionComposter;
    }

    public PartitionCreator getPartitionCreator() {
        return partitionCreator;
    }

    public PartitionStripeProvider getPartitionStripeProvider() {
        return partitionStripeProvider;
    }

    public HighwaterStorage getSystemHighwaterStorage() {
        return highwaterStorage;
    }

    public AmzaAquariumProvider getAquariumProvider() {
        return aquariumProvider;
    }

    public Liveliness getLiveliness() {
        return liveliness;
    }

    public TakeCoordinator getTakeCoordinator() {
        return takeCoordinator;
    }

    public void start(RingMember ringMember, RingHost ringHost) throws Exception {
        partitionIndex.start();
        storageVersionProvider.start();
        indexProviderRegistry.start();
        partitionCreator.init(storageVersionProvider);
        ringStoreReader.start(partitionIndex);

        takeCoordinator.start(ringStoreReader, aquariumProvider);

        HighestPartitionTx takeHighestPartitionTx = (versionedAquarium, highestTxId) -> {
            takeCoordinator.update(ringStoreReader, versionedAquarium.getVersionedPartitionName(), highestTxId);
            return -1;
        };

        systemWALStorage.load(partitionCreator.getSystemPartitions(), takeHighestPartitionTx);

        // start the composter before loading partition stripes in case we need to repair any partitions
        partitionComposter.start();

        partitionStripeProvider.load();

        ringStoreWriter.register(ringMember, ringHost, -1, true);

        partitionStripeProvider.start();
        changeTaker.start();
        partitionTombstoneCompactor.start();

        // last minute initialization
        aquariumProvider.start();
        systemReady.checkReady();
    }

    public void stop() throws Exception {
        aquariumProvider.stop();
        partitionTombstoneCompactor.stop();
        changeTaker.stop();
        partitionStripeProvider.stop();
        partitionComposter.stop();
        takeCoordinator.stop();
        ringStoreReader.stop();
        indexProviderRegistry.stop();
        storageVersionProvider.stop();
        partitionIndex.stop();
    }

    public void migrate(String fromIndexClass, String toIndexClass) throws Exception {
        partitionCreator.streamAllParitions((partitionName, partitionProperties) -> {
            if (partitionProperties.indexClassName.equals(fromIndexClass)) {

                PartitionProperties copy = partitionProperties.copy();
                copy.indexClassName = toIndexClass;
                partitionCreator.updatePartitionProperties(partitionName, copy);
            }
            return true;
        });
    }

    public boolean isReady() {
        return systemReady.isReady();
    }

    @Override
    public long getTimestamp(long timestampId, long deltaMillis) throws Exception {
        if (timestampId <= 0) {
            return 0;
        }
        return orderIdProvider.getApproximateId(timestampId, deltaMillis);
    }

    @Override
    public boolean createPartitionIfAbsent(PartitionName partitionName, PartitionProperties partitionProperties) throws Exception {
        if (partitionCreator.createPartitionIfAbsent(partitionName, partitionProperties)) {
            if (ringStoreReader.isMemberOfRing(partitionName.getRingName(), 0)) {
                partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage1, versionedAquarium) -> {
                    takeCoordinator.stateChanged(ringStoreReader, versionedAquarium.getVersionedPartitionName());
                    return null;
                });
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void awaitOnline(PartitionName partitionName, long timeoutMillis) throws Exception {
        if (!ringStoreWriter.isMemberOfRing(partitionName.getRingName(), 0)) {
            throw new NotARingMemberException("Not a member of the ring for partition: " + partitionName);
        }

        boolean online = false;
        String errorMessage = null;
        long endAfterTimestamp = System.currentTimeMillis() + timeoutMillis;
        do {
            try {
                partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                    versionedAquarium.wipeTheGlass();
                    VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();

                    txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                        partitionCreator.createStoreIfAbsent(versionedPartitionName, stripeIndex);
                        return null;
                    });

                    versionedAquarium.awaitOnline(Math.max(endAfterTimestamp - System.currentTimeMillis(), 0));
                    return null;
                });
                online = true;
                break;
            } catch (PartitionIsExpungedException e) {
                LOG.warn("Awaiting online for expunged partition {}, we will compost and retry", partitionName);
                errorMessage = e.getMessage();
                partitionComposter.compostPartitionIfNecessary(partitionName);
            } catch (PropertiesNotPresentException e) {
                errorMessage = e.getMessage();
                long timeRemaining = endAfterTimestamp - System.currentTimeMillis();
                if (timeRemaining > 0) {
                    Thread.sleep(Math.min(100, Math.max(timeRemaining / 2, 10))); //TODO this is stupid
                }
            }
        }
        while (System.currentTimeMillis() < endAfterTimestamp);

        if (!online) {
            throw new TimeoutException(errorMessage != null ? errorMessage : "Timed out waiting for the partition to come online");
        }
    }

    public AmzaPartitionRoute getPartitionRoute(PartitionName partitionName, long waitForLeaderInMillis) throws Exception {
        RingTopology ring = ringStoreReader.getRing(partitionName.getRingName(), 0);

        long endAfterTimestamp = System.currentTimeMillis() + waitForLeaderInMillis;
        List<RingMemberAndHost> orderedPartitionHosts = new ArrayList<>();
        boolean disposed = false;
        if (ringStoreWriter.isMemberOfRing(partitionName.getRingName(), 0)) {
            do {
                try {
                    partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                        return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                            versionedAquarium.wipeTheGlass();
                            partitionCreator.createStoreIfAbsent(versionedAquarium.getVersionedPartitionName(), stripeIndex);
                            return getPartition(partitionName);
                        });
                    });
                    break;
                } catch (PartitionIsExpungedException e) {
                    LOG.warn("Getting route for expunged partition {}, we will compost and retry", partitionName);
                    partitionComposter.compostPartitionIfNecessary(partitionName);
                } catch (PartitionIsDisposedException e) {
                    LOG.warn("Being asked for a route to a disposed partition:{}", partitionName);
                    disposed = true;
                    break;
                }
            }
            while (System.currentTimeMillis() < endAfterTimestamp);
        }

        if (disposed) {
            return new AmzaPartitionRoute(ring.entries, null, true);
        } else {
            RingMember currentLeader = awaitLeader(partitionName, Math.max(endAfterTimestamp - System.currentTimeMillis(), 0));
            RingMemberAndHost leaderMemberAndHost = null;
            for (RingMemberAndHost entry : ring.entries) {
                RemoteVersionedState remoteVersionedState = partitionStripeProvider.getRemoteVersionedState(entry.ringMember, partitionName);
                if (remoteVersionedState != null && remoteVersionedState.waterline != null) {
                    State state = remoteVersionedState.waterline.getState();
                    if (state == State.leader && entry.ringMember.equals(currentLeader)) {
                        leaderMemberAndHost = entry;
                    }
                    if (state == State.leader || state == State.follower) {
                        orderedPartitionHosts.add(entry);
                    }
                }
            }
            return new AmzaPartitionRoute(orderedPartitionHosts, leaderMemberAndHost, false);
        }
    }

    public void visualizePartition(byte[] rawRingName,
        byte[] rawPartitionName,
        RowStream rowStream) throws Exception {

        PartitionName partitionName = new PartitionName(false, rawRingName, rawPartitionName);
        partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                partitionStripe.takeAllRows(versionedAquarium, rowStream);
                return null;
            });
        });
    }

    public void rebalanceStripes() {

    }

    public static class AmzaPartitionRoute {

        public final List<RingMemberAndHost> orderedMembers;
        public final RingMemberAndHost leader;
        public boolean disposed;

        public AmzaPartitionRoute(List<RingMemberAndHost> orderedMembers, RingMemberAndHost leader, boolean disposed) {
            this.orderedMembers = orderedMembers;
            this.leader = leader;
            this.disposed = disposed;
        }
    }

    @Override
    public Partition getPartition(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new SystemPartition(amzaStats,
                orderIdProvider,
                walUpdated,
                ringStoreReader.getRingMember(),
                partitionName,
                systemWALStorage,
                highwaterStorage,
                ackWaters,
                ringStoreReader,
                takeCoordinator);
        } else {
            return new StripedPartition(amzaStats,
                orderIdProvider,
                partitionCreator,
                walUpdated,
                ringStoreReader.getRingMember(),
                partitionName,
                partitionStripeProvider,
                ackWaters,
                ringStoreReader,
                systemReady,
                takeCoordinator);
        }
    }

    @Override
    public PartitionProperties getProperties(PartitionName partitionName) throws Exception {
        return partitionCreator.getProperties(partitionName);
    }

    @Override
    public void updateProperties(PartitionName partitionName, PartitionProperties partitionProperties) throws Exception {
        partitionCreator.updatePartitionPropertiesIfNecessary(partitionName, partitionProperties);
    }

    @Override
    public RingMember awaitLeader(PartitionName partitionName, long waitForLeaderElection) throws Exception {
        if (partitionName.isSystemPartition()) {
            throw new IllegalArgumentException("System partitions do not have leaders. " + partitionName);
        } else {
            long endAfterTimestamp = System.currentTimeMillis() + waitForLeaderElection;
            do {
                try {
                    Waterline leader = partitionStripeProvider.awaitLeader(partitionName, waitForLeaderElection);
                    return (leader != null) ? RingMember.fromAquariumMember(leader.getMember()) : null;
                } catch (PartitionIsExpungedException e) {
                    LOG.warn("Awaiting leader for expunged partition {}, we will compost and retry", partitionName);
                    partitionComposter.compostPartitionIfNecessary(partitionName);
                }
            }
            while (System.currentTimeMillis() < endAfterTimestamp);
        }
        throw new TimeoutException("Timed out awaiting leader for " + partitionName);
    }

    public boolean hasPartition(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return true;
        } else {
            PartitionStore store = partitionIndex.getSystemPartition(PartitionCreator.REGION_INDEX);
            if (store != null) {
                return store.containsKey(null, partitionName.toBytes());
            }
            return false;
        }
    }

    @Override
    public Iterable<PartitionName> getAllPartitionNames() throws Exception {
        return partitionCreator.getMemberPartitions(null);
    }

    @Override
    public Iterable<PartitionName> getMemberPartitionNames() throws Exception {
        return partitionCreator.getMemberPartitions(ringStoreReader);
    }

    @Override
    public Iterable<PartitionName> getSystemPartitionNames() throws Exception {
        return Iterables.transform(partitionCreator.getSystemPartitions(), VersionedPartitionName::getPartitionName);
    }

    public PartitionProperties getPartitionProperties(PartitionName partitionName) throws Exception {
        return partitionCreator.getProperties(partitionName);
    }

    public boolean promotePartition(PartitionName partitionName) throws Exception {
        return partitionStripeProvider.txPartition(partitionName,
            (txPartitionStripe, highwaterStorage, versionedAquarium) -> versionedAquarium.suggestState(State.leader));
    }

    @Override
    public void destroyPartition(PartitionName partitionName) throws Exception {
        partitionCreator.markForDisposal(partitionName);
    }

    public boolean abandonPartition(PartitionName partitionName) throws Exception {
        VersionedPartitionName versionedPartitionName = partitionStripeProvider.txPartition(partitionName,
            (txPartitionStripe, highwaterStorage1, versionedAquarium) -> {
                LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
                if (livelyEndState.getCurrentState() != State.leader) {
                    return versionedAquarium.getVersionedPartitionName();
                }
                return null;
            });
        if (versionedPartitionName != null) {
            storageVersionProvider.abandonVersion(versionedPartitionName);
            return true;
        }
        return false;
    }

    public void watch(PartitionName partitionName, RowChanges rowChanges) throws Exception {
        if (partitionName.isSystemPartition()) {
            amzaSystemPartitionWatcher.watch(partitionName, rowChanges);
        } else {
            amzaStripedPartitionWatcher.watch(partitionName, rowChanges);
        }
    }

    @Override
    public void availableRowsStream(boolean system,
        ChunkWriteable writeable,
        RingMember remoteRingMember,
        TimestampedRingHost remoteTimestampedRingHost,
        long takeSessionId,
        String sharedKey,
        long heartbeatIntervalMillis) throws Exception {

        ringStoreWriter.register(remoteRingMember, remoteTimestampedRingHost.ringHost, remoteTimestampedRingHost.timestampId, false);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new SnappyOutputStream(out), 8192));

        takeCoordinator.availableRowsStream(system,
            ringStoreReader,
            partitionStripeProvider,
            remoteRingMember,
            takeSessionId,
            sharedKey,
            heartbeatIntervalMillis,
            (partitionName, txId) -> {
                dos.write(1);
                byte[] bytes = partitionName.toBytes();
                dos.writeInt(bytes.length);
                dos.write(bytes);
                dos.writeLong(txId);
            }, () -> {
                if (dos.size() > 0) {
                    dos.flush();
                    byte[] chunk = out.toByteArray();
                    writeable.write(chunk);
                    /*LOG.info("Offered rows for {} length={}", remoteRingMember, chunk.length);*/
                    out.reset();
                }
                return null;
            }, () -> {
                dos.write(1);
                dos.writeInt(0);
                dos.flush();
                writeable.write(out.toByteArray());
                out.reset();
                return null;
            });
        dos.write(0);
        dos.flush();
        writeable.write(out.toByteArray());
    }

    // for testing only
    public void availableRowsStream(boolean system,
        RingMember remoteRingMember,
        TimestampedRingHost remoteTimestampedRingHost,
        long takeSessionId,
        String sharedKey,
        long timeoutMillis,
        AvailableStream availableStream,
        Callable<Void> deliverCallback,
        Callable<Void> pingCallback) throws Exception {
        ringStoreWriter.register(remoteRingMember, remoteTimestampedRingHost.ringHost, remoteTimestampedRingHost.timestampId, false);
        takeCoordinator.availableRowsStream(system,
            ringStoreReader,
            partitionStripeProvider,
            remoteRingMember,
            takeSessionId,
            sharedKey,
            timeoutMillis,
            availableStream,
            deliverCallback,
            pingCallback);
    }

    @Override
    public void rowsStream(DataOutputStream dos,
        RingMember remoteRingMember,
        VersionedPartitionName localVersionedPartitionName,
        long takeSessionId,
        String sharedKey,
        long localTxId,
        long remoteLeadershipToken,
        long limit) throws Exception {

        if (!takeCoordinator.isValidSession(remoteRingMember, takeSessionId, sharedKey)) {
            LOG.warn("Denied stale rowsStream from:{} session:{}", remoteRingMember, takeSessionId);
            throw new IllegalStateException("Attempted to take with invalid session");
        }

        partitionStripeProvider.txPartition(localVersionedPartitionName.getPartitionName(), (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            // TODO could avoid leadership lookup for partitions that have been configs to not care about leadership.
            Waterline leader = versionedAquarium.getLeader();
            long localLeadershipToken = (leader != null) ? leader.getTimestamp() : -1;

            MutableLong bytes = new MutableLong(0);
            VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
            PartitionName partitionName = versionedPartitionName.getPartitionName();

            if (versionedPartitionName.getPartitionVersion() != localVersionedPartitionName.getPartitionVersion()) {
                streamBootstrap(localLeadershipToken, dos, bytes, null, -1, null);
                return null;
            }

            boolean needsToMarkAsKetchup;
            if (localVersionedPartitionName.getPartitionName().isSystemPartition()) {
                needsToMarkAsKetchup = systemWALStorage.takeRowUpdatesSince(versionedPartitionName, localTxId,
                    (_versionedPartitionName, partitionState, rowStreamer) -> {
                        return streamOnline(remoteRingMember,
                            versionedPartitionName,
                            localTxId,
                            localLeadershipToken,
                            limit,
                            dos,
                            bytes,
                            this.highwaterStorage,
                            rowStreamer);
                    });
            } else {
                needsToMarkAsKetchup = txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                    return partitionStripe.takeRowUpdatesSince(versionedAquarium, localTxId,
                        (versionedPartitionName1, livelyEndState, streamer) -> {
                            if (streamer != null) {
                                return streamOnline(remoteRingMember,
                                    versionedPartitionName,
                                    localTxId,
                                    localLeadershipToken,
                                    limit,
                                    dos,
                                    bytes,
                                    highwaterStorage,
                                    streamer);
                            } else {
                                return streamBootstrap(localLeadershipToken, dos, bytes, versionedPartitionName, stripeIndex, livelyEndState);
                            }
                        });
                });
            }

            amzaStats.netStats.wrote.add(bytes.longValue());

            if (needsToMarkAsKetchup) {
                try {
                    versionedAquarium.wipeTheGlass();
                } catch (Exception x) {
                    LOG.warn("Failed to mark as ketchup for partition {}", new Object[] { partitionName }, x);
                }
            }
            return null;
        });
    }

    private boolean streamBootstrap(long leadershipToken,
        DataOutputStream dos,
        MutableLong bytes,
        VersionedPartitionName versionedPartitionName,
        int stripe,
        LivelyEndState livelyEndState) throws Exception {

        dos.writeLong(leadershipToken);
        dos.writeLong(-1);
        dos.writeByte(0); // not online
        dos.writeByte(0); // last entry marker
        dos.writeByte(0); // last entry marker
        dos.writeByte(0); // streamedToEnd marker
        bytes.add(4);
        if (versionedPartitionName == null || livelyEndState == null) {
            // someone thinks we're a member for this partition
            return true;
        } else {
            // BOOTSTRAP'S BOOTSTRAPS!
            partitionCreator.get(versionedPartitionName, stripe);
            return false;
        }
    }

    private boolean streamOnline(RingMember ringMember,
        VersionedPartitionName versionedPartitionName,
        long highestTransactionId,
        long leadershipToken,
        long limit,
        DataOutputStream dos,
        MutableLong bytes,
        HighwaterStorage highwaterStorage,
        PartitionStripe.RowStreamer streamer) throws Exception {

        ackWaters.set(ringMember, versionedPartitionName, highestTransactionId, leadershipToken);
        dos.writeLong(leadershipToken);
        dos.writeLong(versionedPartitionName.getPartitionVersion());
        dos.writeByte(1); // fully online
        bytes.increment();
        RingTopology ring = ringStoreReader.getRing(versionedPartitionName.getPartitionName().getRingName(), -1);
        for (int i = 0; i < ring.entries.size(); i++) {
            if (ring.rootMemberIndex != i) {
                RingMemberAndHost entry = ring.entries.get(i);
                long highwatermark = highwaterStorage.get(entry.ringMember, versionedPartitionName);
                byte[] ringMemberBytes = entry.ringMember.toBytes();
                dos.writeByte(1);
                dos.writeInt(ringMemberBytes.length);
                dos.write(ringMemberBytes);
                dos.writeLong(highwatermark);
                bytes.add(1 + 4 + ringMemberBytes.length + 8);
            }
        }

        dos.writeByte(0); // last entry marker
        bytes.increment();

        long[] limited = new long[1];
        long[] lastRowTxId = { -1 };
        boolean streamedToEnd = streamer.stream((rowFP, rowTxId, rowType, row) -> {
            if (limited[0] >= limit && lastRowTxId[0] < rowTxId) {
                return false;
            }
            lastRowTxId[0] = rowTxId;
            dos.writeByte(1);
            dos.writeLong(rowTxId);
            dos.writeByte(rowType.toByte());
            dos.writeInt(row.length);
            dos.write(row);
            bytes.add(1 + 8 + 1 + 4 + row.length);
            limited[0]++;
            return true;
        });

        dos.writeByte(0); // last entry marker
        bytes.increment();
        dos.writeByte(streamedToEnd ? 1 : 0); // streamedToEnd marker
        bytes.increment();
        return false;
    }

    @Override
    public void rowsTaken(RingMember remoteRingMember,
        long takeSessionId,
        String sharedKey,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId,
        long leadershipToken) throws Exception {
        ackWaters.set(remoteRingMember, localVersionedPartitionName, localTxId, leadershipToken);

        partitionStripeProvider.txPartition(localVersionedPartitionName.getPartitionName(), (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
            if (versionedPartitionName.getPartitionVersion() == localVersionedPartitionName.getPartitionVersion()) {
                takeCoordinator.rowsTaken(remoteRingMember, takeSessionId, sharedKey, txPartitionStripe, versionedAquarium, localTxId);
            }
            return null;
        });
    }

    @Override
    public void pong(RingMember remoteRingMember, long takeSessionId, String sharedKey) throws Exception {
        takeCoordinator.pong(remoteRingMember, takeSessionId, sharedKey);
    }

    @Override
    public void invalidate(RingMember remoteRingMember,
        long takeSessionId,
        String takeSharedKey,
        VersionedPartitionName versionedPartitionName) throws Exception {
        takeCoordinator.invalidate(ringStoreReader, remoteRingMember, takeSessionId, takeSharedKey, versionedPartitionName);
    }

    public void compactAllTombstones() throws Exception {
        LOG.info("Manual compact all tombstones requests.");

        ExecutorService compactorPool = new ThreadPoolExecutor(numberOfStripes, numberOfStripes,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("compactor-%d").build());

        try {
            List<Future<?>> runnables = Lists.newArrayList();
            for (int i = 0; i < numberOfStripes; i++) {
                int stripe = i;
                runnables.add(compactorPool.submit(() -> {
                    partitionTombstoneCompactor.compactTombstone(true, stripe);
                    return null;
                }));
            }
            for (Future<?> runnable : runnables) {
                runnable.get();
            }
        } finally {
            compactorPool.shutdown();
        }
    }

    public void mergeAllDeltas(boolean force) {
        partitionStripeProvider.mergeAll(force);
    }

    public void expunge() throws Exception {
        partitionComposter.compostAll();
    }
}
