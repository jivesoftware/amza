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
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.partition.RemoteVersionedState;
import com.jivesoftware.os.amza.service.partition.TxHighestPartitionTx;
import com.jivesoftware.os.amza.service.replication.AmzaAquariumProvider;
import com.jivesoftware.os.amza.service.replication.PartitionComposter;
import com.jivesoftware.os.amza.service.replication.PartitionStateStorage;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.PartitionTombstoneCompactor;
import com.jivesoftware.os.amza.service.replication.RowChangeTaker;
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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
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
    private final AmzaRingStoreReader ringStoreReader;
    private final AmzaRingStoreWriter ringStoreWriter;
    private final AckWaters ackWaters;
    private final SystemWALStorage systemWALStorage;
    private final HighwaterStorage systemHighwaterStorage;
    private final TakeCoordinator takeCoordinator;
    private final PartitionStateStorage partitionStateStorage;
    private final RowChangeTaker changeTaker;
    private final PartitionTombstoneCompactor partitionTombstoneCompactor;
    private final PartitionComposter partitionComposter;
    private final PartitionIndex partitionIndex;
    private final PartitionCreator partitionCreator;
    private final PartitionStripeProvider partitionStripeProvider;
    private final WALUpdated walUpdated;
    private final AmzaPartitionWatcher partitionWatcher;
    private final AmzaAquariumProvider aquariumProvider;
    private final AmzaSystemReady systemReady;
    private final Liveliness liveliness;
    private final TxHighestPartitionTx<Long> txHighestPartitionTx;

    public AmzaService(TimestampedOrderIdProvider orderIdProvider,
        AmzaStats amzaStats,
        AmzaRingStoreReader ringReader,
        AmzaRingStoreWriter amzaHostRing,
        AckWaters ackWaters,
        SystemWALStorage systemWALStorage,
        HighwaterStorage systemHighwaterStorage,
        TakeCoordinator takeCoordinator,
        PartitionStateStorage partitionStateStorage,
        RowChangeTaker changeTaker,
        PartitionTombstoneCompactor partitionTombstoneCompactor,
        PartitionComposter partitionComposter,
        PartitionIndex partitionIndex,
        PartitionCreator partitionCreator,
        PartitionStripeProvider partitionStripeProvider,
        WALUpdated walUpdated,
        AmzaPartitionWatcher partitionWatcher,
        AmzaAquariumProvider aquariumProvider,
        AmzaSystemReady systemReady,
        Liveliness liveliness) {

        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.ringStoreReader = ringReader;
        this.ringStoreWriter = amzaHostRing;
        this.ackWaters = ackWaters;
        this.systemWALStorage = systemWALStorage;
        this.systemHighwaterStorage = systemHighwaterStorage;
        this.takeCoordinator = takeCoordinator;
        this.partitionStateStorage = partitionStateStorage;
        this.changeTaker = changeTaker;
        this.partitionTombstoneCompactor = partitionTombstoneCompactor;
        this.partitionComposter = partitionComposter;
        this.partitionIndex = partitionIndex;
        this.partitionCreator = partitionCreator;
        this.partitionStripeProvider = partitionStripeProvider;
        this.walUpdated = walUpdated;
        this.partitionWatcher = partitionWatcher;
        this.aquariumProvider = aquariumProvider;
        this.systemReady = systemReady;
        this.liveliness = liveliness;
        this.txHighestPartitionTx = (versionedAquarium, highestPartitionTx) -> {
            VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
            PartitionName partitionName = versionedPartitionName.getPartitionName();
            if (partitionName.isSystemPartition()) {
                return systemWALStorage.highestPartitionTxId(versionedPartitionName, highestPartitionTx);
            } else {
                return partitionStripeProvider.txPartition(partitionName,
                    (stripe, highwaterStorage) -> stripe.highestAquariumTxId(versionedAquarium, highestPartitionTx));
            }
        };
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

    public PartitionStateStorage getPartitionStateStorage() {
        return partitionStateStorage;
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
        return systemHighwaterStorage;
    }

    public AmzaAquariumProvider getAquariumProvider() {
        return aquariumProvider;
    }

    public Liveliness getLiveliness() {
        return liveliness;
    }

    public void start() throws Exception {
        partitionStripeProvider.start();
        changeTaker.start();
        partitionTombstoneCompactor.start();
        partitionComposter.start();

        // last minute initialization
        aquariumProvider.start();
        systemReady.checkReady();
    }

    public void stop() throws Exception {
        partitionStripeProvider.stop();
        changeTaker.stop();
        partitionTombstoneCompactor.stop();
        partitionComposter.stop();
    }

    @Override
    public long getTimestamp(long timestampId, long deltaMillis) throws Exception {
        if (timestampId <= 0) {
            return 0;
        }
        return orderIdProvider.getApproximateId(timestampId, deltaMillis);
    }

    @Override
    public void setPropertiesIfAbsent(PartitionName partitionName, PartitionProperties partitionProperties) throws Exception {
        PartitionProperties properties = partitionIndex.getProperties(partitionName);
        if (properties == null) {
            partitionCreator.updatePartitionProperties(partitionName, partitionProperties);
        }
    }

    @Override
    public void awaitOnline(PartitionName partitionName, long timeoutMillis) throws Exception {
        PartitionProperties properties = partitionIndex.getProperties(partitionName);
        if (properties == null) {
            throw new PropertiesNotPresentException("No properties for partition: " + partitionName);
        }

        if (!ringStoreWriter.isMemberOfRing(partitionName.getRingName())) {
            throw new NotARingMemberException("Not a member of the ring for partition: " + partitionName);
        }

        long endAfterTimestamp = System.currentTimeMillis() + timeoutMillis;
        do {
            try {
                partitionStateStorage.tx(partitionName, versionedAquarium -> {
                    versionedAquarium.wipeTheGlass();
                    VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
                    if (partitionCreator.createPartitionStoreIfAbsent(versionedPartitionName, properties)) {
                        takeCoordinator.stateChanged(ringStoreReader, versionedPartitionName);
                    }
                    versionedAquarium.awaitOnline(Math.max(endAfterTimestamp - System.currentTimeMillis(), 0));
                    //LOG.info("Finished awaiting online in {} ms", System.currentTimeMillis() - start);
                    return null;
                });
                break;
            } catch (PartitionIsExpungedException e) {
                LOG.warn("Awaiting online for expunged partition {}, we will compost and retry", partitionName);
                partitionComposter.compostPartitionIfNecessary(partitionName);
            }
        }
        while (System.currentTimeMillis() < endAfterTimestamp);
    }

    public AmzaPartitionRoute getPartitionRoute(PartitionName partitionName, long waitForLeaderInMillis) throws Exception {

        RingTopology ring = ringStoreReader.getRing(partitionName.getRingName());
        PartitionProperties properties = partitionIndex.getProperties(partitionName);
        if (properties == null) {
            return new AmzaPartitionRoute(ring.entries, null);
        }

        long endAfterTimestamp = System.currentTimeMillis() + waitForLeaderInMillis;
        List<RingMemberAndHost> orderedPartitionHosts = new ArrayList<>();
        if (ringStoreWriter.isMemberOfRing(partitionName.getRingName())) {
            do {
                try {
                    partitionStateStorage.tx(partitionName, versionedAquarium -> {
                        versionedAquarium.wipeTheGlass();
                        partitionCreator.createPartitionStoreIfAbsent(versionedAquarium.getVersionedPartitionName(), properties);
                        return getPartition(partitionName);
                    });
                    break;
                } catch (PartitionIsExpungedException e) {
                    LOG.warn("Getting route for expunged partition {}, we will compost and retry", partitionName);
                    partitionComposter.compostPartitionIfNecessary(partitionName);
                }
            }
            while (System.currentTimeMillis() < endAfterTimestamp);
        }

        awaitLeader(partitionName, Math.max(endAfterTimestamp - System.currentTimeMillis(), 0));

        RingMemberAndHost leader = null;
        for (RingMemberAndHost entry : ring.entries) {
            RemoteVersionedState remoteVersionedState = partitionStateStorage.getRemoteVersionedState(entry.ringMember, partitionName);
            if (remoteVersionedState != null && remoteVersionedState.waterline != null) {
                State state = remoteVersionedState.waterline.getState();
                if (state == State.leader) {
                    leader = entry;
                }
                if (state == State.leader || state == State.follower) {
                    orderedPartitionHosts.add(entry);
                }
            }
        }
        return new AmzaPartitionRoute(orderedPartitionHosts, leader);
    }

    public void visualizePartition(byte[] rawRingName, byte[] rawPartitionName, RowStream rowStream) throws Exception {
        PartitionName partitionName = new PartitionName(false, rawRingName, rawPartitionName);
        partitionStripeProvider.txPartition(partitionName, (stripe, highwaterStorage) -> {
            stripe.takeAllRows(partitionName, rowStream);
            return null;
        });
    }

    public static class AmzaPartitionRoute {

        public final List<RingMemberAndHost> orderedMembers;
        public final RingMemberAndHost leader;

        public AmzaPartitionRoute(List<RingMemberAndHost> orderedMembers, RingMemberAndHost leader) {
            this.orderedMembers = orderedMembers;
            this.leader = leader;
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
                systemHighwaterStorage,
                ackWaters,
                ringStoreReader);
        } else {
            return new StripedPartition(amzaStats,
                orderIdProvider,
                partitionIndex,
                walUpdated,
                ringStoreReader.getRingMember(),
                partitionName,
                partitionStripeProvider,
                ackWaters,
                ringStoreReader,
                systemReady);
        }
    }

    @Override
    public RingMember awaitLeader(PartitionName partitionName, long waitForLeaderElection) throws Exception {
        if (partitionName.isSystemPartition()) {
            throw new IllegalArgumentException("System partitions do not have leaders. " + partitionName);
        } else {
            long endAfterTimestamp = System.currentTimeMillis() + waitForLeaderElection;
            do {
                try {
                    Waterline leader = partitionStateStorage.awaitLeader(partitionName, waitForLeaderElection);
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
            PartitionStore store = partitionIndex.get(PartitionCreator.REGION_INDEX);
            if (store != null) {
                return store.containsKey(null, partitionName.toBytes());
            }
            return false;
        }
    }

    @Override
    public Set<PartitionName> getAllPartitionNames() throws Exception {
        return Sets.newHashSet(partitionIndex.getAllPartitions());
    }

    @Override
    public Set<PartitionName> getMemberPartitionNames() throws Exception {
        return Sets.newHashSet(Iterables.transform(partitionIndex.getMemberPartitions(), VersionedPartitionName::getPartitionName));
    }

    public PartitionProperties getPartitionProperties(PartitionName partitionName) throws Exception {
        return partitionIndex.getProperties(partitionName);
    }

    public boolean promotePartition(PartitionName partitionName) throws Exception {
        return partitionStateStorage.tx(partitionName, versionedAquarium -> versionedAquarium.suggestState(State.leader));
    }

    @Override
    public void destroyPartition(PartitionName partitionName) throws Exception {
        partitionCreator.markForDisposal(partitionName);
    }

    public void watch(PartitionName partitionName, RowChanges rowChanges) throws Exception {
        partitionWatcher.watch(partitionName, rowChanges);
    }

    public RowChanges unwatch(PartitionName partitionName) throws Exception {
        return partitionWatcher.unwatch(partitionName);
    }

    @Override
    public void availableRowsStream(ChunkWriteable writeable,
        RingMember remoteRingMember,
        TimestampedRingHost remoteTimestampedRingHost,
        long takeSessionId,
        long heartbeatIntervalMillis) throws Exception {

        ringStoreWriter.register(remoteRingMember, remoteTimestampedRingHost.ringHost, remoteTimestampedRingHost.timestampId);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(new SnappyOutputStream(out));

        takeCoordinator.availableRowsStream(txHighestPartitionTx,
            ringStoreReader,
            partitionStateStorage,
            remoteRingMember,
            takeSessionId,
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
    public void availableRowsStream(RingMember remoteRingMember,
        TimestampedRingHost remoteTimestampedRingHost,
        long takeSessionId,
        long timeoutMillis,
        AvailableStream availableStream,
        Callable<Void> deliverCallback,
        Callable<Void> pingCallback) throws Exception {
        ringStoreWriter.register(remoteRingMember, remoteTimestampedRingHost.ringHost, remoteTimestampedRingHost.timestampId);
        takeCoordinator.availableRowsStream(txHighestPartitionTx,
            ringStoreReader,
            partitionStateStorage,
            remoteRingMember,
            takeSessionId,
            timeoutMillis,
            availableStream,
            deliverCallback,
            pingCallback);
    }

    @Override
    public void rowsStream(DataOutputStream dos,
        RingMember remoteRingMember,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId,
        long remoteLeadershipToken) throws Exception {

        partitionStateStorage.tx(localVersionedPartitionName.getPartitionName(), versionedAquarium -> {
            // TODO could avoid leadership lookup for partitions that have been configs to not care about leadership.
            Waterline leader = versionedAquarium.getLeader();
            long localLeadershipToken = (leader != null) ? leader.getTimestamp() : -1;

            MutableLong bytes = new MutableLong(0);
            VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
            PartitionName partitionName = versionedPartitionName.getPartitionName();

            if (versionedPartitionName.getPartitionVersion() != localVersionedPartitionName.getPartitionVersion()) {
                streamBootstrap(localLeadershipToken, dos, bytes, null, null);
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
                            dos,
                            bytes,
                            systemHighwaterStorage,
                            rowStreamer);
                    });
            } else {
                needsToMarkAsKetchup = partitionStripeProvider.txPartition(partitionName,
                    (stripe, highwaterStorage) -> stripe.takeRowUpdatesSince(versionedAquarium, localTxId,
                        (versionedPartitionName1, livelyEndState, streamer) -> {
                            if (streamer != null) {
                                return streamOnline(remoteRingMember,
                                    versionedPartitionName,
                                    localTxId,
                                    localLeadershipToken,
                                    dos,
                                    bytes,
                                    highwaterStorage,
                                    streamer);
                            } else {
                                return streamBootstrap(localLeadershipToken, dos, bytes, versionedPartitionName, livelyEndState);
                            }
                        }));
            }

            amzaStats.netStats.wrote.addAndGet(bytes.longValue());

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
        LivelyEndState livelyEndState) throws Exception {

        dos.writeLong(leadershipToken);
        dos.writeLong(-1);
        dos.writeByte(0); // not online
        dos.writeByte(0); // last entry marker
        dos.writeByte(0); // last entry marker
        bytes.add(3);
        if (versionedPartitionName == null || livelyEndState == null) {
            // someone thinks we're a member for this partition
            return true;
        } else {
            // BOOTSTRAP'S BOOTSTRAPS!
            partitionIndex.get(versionedPartitionName);
            return false;
        }
    }

    private boolean streamOnline(RingMember ringMember,
        VersionedPartitionName versionedPartitionName,
        long highestTransactionId,
        long leadershipToken,
        DataOutputStream dos,
        MutableLong bytes,
        HighwaterStorage highwaterStorage,
        PartitionStripe.RowStreamer streamer) throws Exception {

        ackWaters.set(ringMember, versionedPartitionName, highestTransactionId, leadershipToken);
        dos.writeLong(leadershipToken);
        dos.writeLong(versionedPartitionName.getPartitionVersion());
        dos.writeByte(1); // fully online
        bytes.increment();
        RingTopology ring = ringStoreReader.getRing(versionedPartitionName.getPartitionName().getRingName());
        for (int i = 0; i < ring.entries.size(); i++) {
            if (ring.rootMemberIndex != i) {
                RingMemberAndHost entry = ring.entries.get(i);
                Long highwatermark = highwaterStorage.get(entry.ringMember, versionedPartitionName);
                if (highwatermark != null) {
                    byte[] ringMemberBytes = entry.ringMember.toBytes();
                    dos.writeByte(1);
                    dos.writeInt(ringMemberBytes.length);
                    dos.write(ringMemberBytes);
                    dos.writeLong(highwatermark);
                    bytes.add(1 + 4 + ringMemberBytes.length + 8);
                }
            }
        }

        dos.writeByte(0); // last entry marker
        bytes.increment();

        streamer.stream((rowFP, rowTxId, rowType, row) -> {
            dos.writeByte(1);
            dos.writeLong(rowTxId);
            dos.writeByte(rowType.toByte());
            dos.writeInt(row.length);
            dos.write(row);
            bytes.add(1 + 8 + 1 + 4 + row.length);
            return true;
        });

        dos.writeByte(0); // last entry marker
        bytes.increment();
        return false;
    }

    @Override
    public void rowsTaken(RingMember remoteRingMember,
        long takeSessionId,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId,
        long leadershipToken) throws Exception {
        ackWaters.set(remoteRingMember, localVersionedPartitionName, localTxId, leadershipToken);

        partitionStateStorage.tx(localVersionedPartitionName.getPartitionName(), versionedAquarium -> {
            VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
            if (versionedPartitionName.getPartitionVersion() == localVersionedPartitionName.getPartitionVersion()) {
                takeCoordinator.rowsTaken(txHighestPartitionTx, remoteRingMember, takeSessionId, versionedAquarium, localTxId);
            }
            return null;
        });
    }

    public void compactAllTombstones() throws Exception {
        LOG.info("Manual compact all tombstones requests.");
        partitionTombstoneCompactor.compactTombstone(-1, partitionTombstoneCompactor.removeIfOlderThanTimestampId(), true);
    }

    public void mergeAllDeltas(boolean force) {
        partitionStripeProvider.mergeAll(force);
    }

    public void expunge() throws Exception {
        partitionComposter.compostAll();
    }
}
