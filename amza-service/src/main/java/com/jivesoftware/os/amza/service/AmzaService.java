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
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.service.replication.PartitionComposter;
import com.jivesoftware.os.amza.service.replication.PartitionStatusStorage;
import com.jivesoftware.os.amza.shared.partition.RemoteVersionedStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedStatus;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.replication.PartitionTombstoneCompactor;
import com.jivesoftware.os.amza.shared.partition.RemoteVersionedStatus;
import com.jivesoftware.os.amza.service.replication.RowChangeTaker;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.AckWaters;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPIProvider;
import com.jivesoftware.os.amza.shared.ChunkWriteable;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.TakeCoordinator;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * Amza pronounced (AH m z ah )
 * Sanskrit word meaning partition / share.
 */
public class AmzaService implements AmzaInstance, AmzaPartitionAPIProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TimestampedOrderIdProvider orderIdProvider;
    private final AmzaStats amzaStats;
    private final AmzaRingStoreReader ringStoreReader;
    private final AmzaRingStoreWriter ringStoreWriter;
    private final AckWaters ackWaters;
    private final SystemWALStorage systemWALStorage;
    private final HighwaterStorage systemHighwaterStorage;
    private final TakeCoordinator takeCoordinator;
    private final PartitionStatusStorage partitionStatusStorage;
    private final RowChangeTaker changeTaker;
    private final PartitionTombstoneCompactor partitionTombstoneCompactor;
    private final PartitionComposter partitionComposter;
    private final PartitionIndex partitionIndex;
    private final PartitionProvider partitionProvider;
    private final PartitionStripeProvider partitionStripeProvider;
    private final WALUpdated walUpdated;
    private final AmzaPartitionWatcher partitionWatcher;

    public AmzaService(TimestampedOrderIdProvider orderIdProvider,
        AmzaStats amzaStats,
        AmzaRingStoreReader ringReader,
        AmzaRingStoreWriter amzaHostRing,
        AckWaters ackWaters,
        SystemWALStorage systemWALStorage,
        HighwaterStorage systemHighwaterStorage,
        TakeCoordinator takeCoordinator,
        PartitionStatusStorage partitionStatusStorage,
        RowChangeTaker changeTaker,
        PartitionTombstoneCompactor partitionTombstoneCompactor,
        PartitionComposter partitionComposter,
        PartitionIndex partitionIndex,
        PartitionProvider partitionProvider,
        PartitionStripeProvider partitionStripeProvider,
        WALUpdated walUpdated,
        AmzaPartitionWatcher partitionWatcher) {
        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.ringStoreReader = ringReader;
        this.ringStoreWriter = amzaHostRing;
        this.ackWaters = ackWaters;
        this.systemWALStorage = systemWALStorage;
        this.systemHighwaterStorage = systemHighwaterStorage;
        this.takeCoordinator = takeCoordinator;
        this.partitionStatusStorage = partitionStatusStorage;
        this.changeTaker = changeTaker;
        this.partitionTombstoneCompactor = partitionTombstoneCompactor;
        this.partitionComposter = partitionComposter;
        this.partitionIndex = partitionIndex;
        this.partitionProvider = partitionProvider;
        this.partitionStripeProvider = partitionStripeProvider;
        this.walUpdated = walUpdated;
        this.partitionWatcher = partitionWatcher;
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

    public PartitionStatusStorage getPartitionStatusStorage() {
        return partitionStatusStorage;
    }

    public PartitionComposter getPartitionComposter() {
        return partitionComposter;
    }

    public PartitionProvider getPartitionProvider() {
        return partitionProvider;
    }

    public PartitionStripeProvider getPartitionStripeProvider() {
        return partitionStripeProvider;
    }

    public HighwaterStorage getSystemHighwaterStorage() {
        return systemHighwaterStorage;
    }

    public void start() throws Exception {
        partitionStripeProvider.start();
        changeTaker.start();
        partitionTombstoneCompactor.start();
        partitionComposter.start();
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

    public void setPropertiesIfAbsent(PartitionName partitionName, PartitionProperties partitionProperties) throws Exception {
        PartitionProperties properties = partitionIndex.getProperties(partitionName);
        if (properties == null) {
            partitionProvider.updatePartitionProperties(partitionName, partitionProperties);
        }
    }

    public void awaitOnline(PartitionName partitionName, long timeoutMillis) throws Exception {
        if (!ringStoreWriter.isMemberOfRing(partitionName.getRingName())) {
            throw new IllegalStateException("Not a member of the ring: " + partitionName.getRingName());
        }

        PartitionProperties properties = partitionIndex.getProperties(partitionName);
        if (properties == null) {
            throw new IllegalStateException("No properties for partition: " + partitionName);
        }
        partitionStatusStorage.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            if (versionedPartitionName == null) {
                VersionedStatus versionedStatus = partitionStatusStorage.markAsKetchup(partitionName);
                versionedPartitionName = new VersionedPartitionName(partitionName, versionedStatus.version);
                partitionStatus = versionedStatus.status;
                if (properties.takeFromFactor == 0) {
                    partitionStatusStorage.markAsOnline(versionedPartitionName);
                }
            }
            if (partitionProvider.createPartitionStoreIfAbsent(versionedPartitionName, properties)) {
                takeCoordinator.statusChanged(ringStoreReader, versionedPartitionName, partitionStatus);
            }
            return null;
        });

        partitionStatusStorage.awaitOnline(partitionName, timeoutMillis);
    }

    public AmzaPartitionRoute getPartitionRoute(PartitionName partitionName) throws Exception {

        NavigableMap<RingMember, RingHost> ring = ringStoreReader.getRing(partitionName.getRingName());
        PartitionProperties properties = partitionIndex.getProperties(partitionName);
        if (properties == null) {
            return new AmzaPartitionRoute(new ArrayList<>(ring.values()),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
        }

        List<RingHost> orderedPartitionHosts = new ArrayList<>();
        List<RingMember> unregisteredRingMembers = new ArrayList<>();
        List<RingMember> ketchupRingMembers = new ArrayList<>();
        List<RingMember> expungedRingMembers = new ArrayList<>();
        List<RingMember> missingRingMembers = new ArrayList<>();

        // TODO do we still need to do this?
        if (ringStoreWriter.isMemberOfRing(partitionName.getRingName())) {
            partitionStatusStorage.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
                if (versionedPartitionName == null) {
                    VersionedStatus versionedStatus = partitionStatusStorage.markAsKetchup(partitionName);
                    versionedPartitionName = new VersionedPartitionName(partitionName, versionedStatus.version);
                    if (properties.takeFromFactor == 0) {
                        partitionStatusStorage.markAsOnline(versionedPartitionName);
                    }
                }
                partitionProvider.createPartitionStoreIfAbsent(versionedPartitionName, properties);
                return getPartition(partitionName);
            });
        }

        for (Entry<RingMember, RingHost> e : ring.entrySet()) {
            if (e.getValue() == RingHost.UNKNOWN_RING_HOST) {
                unregisteredRingMembers.add(e.getKey());
            }
            RemoteVersionedStatus remoteVersionedStatus = partitionStatusStorage.getRemoteStatus(e.getKey(), partitionName);
            if (remoteVersionedStatus == null) {
                missingRingMembers.add(e.getKey());
            } else if (remoteVersionedStatus.status == TxPartitionStatus.Status.EXPUNGE) {
                expungedRingMembers.add(e.getKey());
            } else if (remoteVersionedStatus.status == TxPartitionStatus.Status.KETCHUP) {
                ketchupRingMembers.add(e.getKey());
            } else {
                orderedPartitionHosts.add(e.getValue());
            }
        }
        return new AmzaPartitionRoute(Collections.emptyList(), orderedPartitionHosts, unregisteredRingMembers, ketchupRingMembers, expungedRingMembers,
            missingRingMembers);
    }

    public static class AmzaPartitionRoute {

        public List<RingHost> uninitializedHosts;
        public List<RingHost> orderedPartitionHosts;
        public List<RingMember> unregisteredRingMembers;
        public List<RingMember> ketchupRingMembers;
        public List<RingMember> expungedRingMembers;
        public List<RingMember> missingRingMembers;

        public AmzaPartitionRoute(List<RingHost> uninitializedHosts,
            List<RingHost> orderedPartitionHosts,
            List<RingMember> unregisteredRingMembers,
            List<RingMember> ketchupRingMembers,
            List<RingMember> expungedRingMembers,
            List<RingMember> missingRingMembers) {
            this.uninitializedHosts = uninitializedHosts;
            this.orderedPartitionHosts = orderedPartitionHosts;
            this.unregisteredRingMembers = unregisteredRingMembers;
            this.ketchupRingMembers = ketchupRingMembers;
            this.expungedRingMembers = expungedRingMembers;
            this.missingRingMembers = missingRingMembers;
        }

    }

    @Override
    public AmzaPartitionAPI getPartition(PartitionName partitionName) throws Exception {
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
                walUpdated,
                ringStoreReader.getRingMember(),
                partitionName,
                partitionStripeProvider,
                ackWaters,
                ringStoreReader);
        }
    }

    public boolean hasPartition(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return true;
        } else {
            PartitionStore store = partitionIndex.get(PartitionProvider.REGION_INDEX);
            if (store != null) {
                return store.containsKey(partitionName.toBytes());
            }
            return false;
        }
    }

    @Override
    public Set<PartitionName> getPartitionNames() {
        return Sets.newHashSet(Lists.newArrayList(Iterables.transform(partitionIndex.getAllPartitions(), VersionedPartitionName::getPartitionName)));
    }

    public PartitionProperties getPartitionProperties(PartitionName partitionName) throws Exception {
        return partitionIndex.getProperties(partitionName);
    }

    @Override
    public void destroyPartition(PartitionName partitionName) throws Exception {

        NavigableMap<RingMember, RingHost> ring = ringStoreReader.getRing(partitionName.getRingName());
        if (ring.containsKey(ringStoreReader.getRingMember())) {
            RemoteVersionedStatus remoteVersionedStatus = partitionStatusStorage.getRemoteStatus(ringStoreReader.getRingMember(), partitionName);
            if (remoteVersionedStatus != null) {
                LOG.info("Handling request to destroy partitionName:{}", partitionName);
                for (RingMember ringMember : ring.keySet()) {
                    VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, remoteVersionedStatus.version);
                    VersionedStatus versionedStatus = partitionStatusStorage.markForDisposal(versionedPartitionName, ringMember);
                    LOG.info("Destroyed partitionName:{} versionedStatus:{} for ringMember:{}", versionedPartitionName, versionedStatus, ringMember);
                }
            }
        }
    }

    public void watch(PartitionName partitionName, RowChanges rowChanges) throws Exception {
        partitionWatcher.watch(partitionName, rowChanges);
    }

    public RowChanges unwatch(PartitionName partitionName) throws Exception {
        return partitionWatcher.unwatch(partitionName);
    }

    @Override
    public void availableRowsStream(ChunkWriteable writeable, RingMember remoteRingMember, long takeSessionId, long heartbeatIntervalMillis) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        takeCoordinator.availableRowsStream(ringStoreReader, remoteRingMember, takeSessionId, heartbeatIntervalMillis, (partitionName, status, txId) -> {
            dos.write(1);
            byte[] bytes = partitionName.toBytes();
            dos.writeInt(bytes.length);
            dos.write(bytes);
            dos.write(status.getSerializedForm());
            dos.writeLong(txId);
        }, () -> {
            if (out.size() > 0) {
                writeable.write(out.toByteArray());
                out.reset();
            }
            return null;
        }, () -> {
            dos.write(1);
            dos.writeInt(0);
            writeable.write(out.toByteArray());
            out.reset();
            return null;
        });
        dos.write(0);
        writeable.write(out.toByteArray());
    }

    // for testing only
    public void availableRowsStream(RingMember remoteRingMember,
        long takeSessionId,
        long timeoutMillis,
        AvailableStream availableStream,
        Callable<Void> deliverCallback,
        Callable<Void> pingCallback) throws Exception {
        takeCoordinator.availableRowsStream(ringStoreReader, remoteRingMember, takeSessionId, timeoutMillis, availableStream, deliverCallback, pingCallback);
    }

    @Override
    public void rowsStream(DataOutputStream dos,
        RingMember remoteRingMember,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId) throws Exception {

        MutableLong bytes = new MutableLong(0);
        boolean needsToMarkAsKetchup;
        if (localVersionedPartitionName.getPartitionName().isSystemPartition()) {
            needsToMarkAsKetchup = systemWALStorage.takeRowUpdatesSince(localVersionedPartitionName, localTxId,
                (versionedPartitionName, partitionStatus, rowStreamer) -> {
                    return streamOnline(remoteRingMember,
                        versionedPartitionName,
                        localTxId,
                        dos,
                        bytes,
                        systemHighwaterStorage,
                        rowStreamer);
                });
        } else {
            needsToMarkAsKetchup = partitionStripeProvider.txPartition(localVersionedPartitionName.getPartitionName(), (stripe, highwaterStorage) -> {
                return stripe.takeRowUpdatesSince(localVersionedPartitionName.getPartitionName(), localTxId,
                    (versionedPartitionName, partitionStatus, streamer) -> {
                        if (localVersionedPartitionName.equals(versionedPartitionName) && partitionStatus == TxPartitionStatus.Status.ONLINE) {
                            return streamOnline(remoteRingMember,
                                versionedPartitionName,
                                localTxId,
                                dos,
                                bytes,
                                highwaterStorage,
                                streamer);
                        } else {
                            return streamOffline(dos, bytes, versionedPartitionName, partitionStatus);
                        }
                    });
            });
        }

        amzaStats.netStats.wrote.addAndGet(bytes.longValue());

        if (needsToMarkAsKetchup) {
            PartitionName partitionName = localVersionedPartitionName.getPartitionName();
            try {
                if (ringStoreWriter.isMemberOfRing(partitionName.getRingName()) && partitionProvider.hasPartition(partitionName)) {
                    partitionStatusStorage.markAsKetchup(partitionName);
                }
            } catch (Exception x) {
                LOG.warn("Failed to mark as ketchup for partition {}", new Object[]{partitionName}, x);
            }
        }
    }

    private boolean streamOffline(DataOutputStream dos, MutableLong bytes, VersionedPartitionName versionedPartitionName,
        TxPartitionStatus.Status partitionStatus)
        throws IOException, Exception {
        dos.writeLong(-1);
        dos.writeByte(0); // not online
        dos.writeByte(0); // last entry marker
        dos.writeByte(0); // last entry marker
        bytes.add(3);
        if (versionedPartitionName == null || partitionStatus == null) {
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
        DataOutputStream dos,
        MutableLong bytes,
        HighwaterStorage highwaterStorage,
        PartitionStripe.RowStreamer streamer) throws Exception {

        ackWaters.set(ringMember, versionedPartitionName, highestTransactionId);
        dos.writeLong(versionedPartitionName.getPartitionVersion());
        dos.writeByte(1); // fully online
        bytes.increment();
        for (Entry<RingMember, RingHost> node : ringStoreReader.getNeighbors(versionedPartitionName.getPartitionName().getRingName())) {
            Long highwatermark = highwaterStorage.get(node.getKey(), versionedPartitionName);
            if (highwatermark != null) {
                byte[] ringMemberBytes = node.getKey().toBytes();
                dos.writeByte(1);
                dos.writeInt(ringMemberBytes.length);
                dos.write(ringMemberBytes);
                dos.writeLong(highwatermark);
                bytes.add(1 + 4 + ringMemberBytes.length + 8);
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
        VersionedPartitionName localVersionedPartitionName,
        long localTxId) throws Exception {
        ackWaters.set(remoteRingMember, localVersionedPartitionName, localTxId);
        takeCoordinator.rowsTaken(remoteRingMember, localVersionedPartitionName, localTxId);
    }

    public void compactAllTombstones() throws Exception {
        partitionTombstoneCompactor.compactTombstone(1, 1, partitionTombstoneCompactor.removeIfOlderThanTimestmapId());
    }

    public void mergeAllDeltas(boolean force) {
        partitionStripeProvider.mergeAll(force);
    }

    public void expunge() throws Exception {
        partitionComposter.compost();
    }
}
