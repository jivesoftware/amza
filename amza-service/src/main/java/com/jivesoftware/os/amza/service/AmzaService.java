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
import com.jivesoftware.os.amza.service.replication.PartitionChangeTaker;
import com.jivesoftware.os.amza.service.replication.PartitionCompactor;
import com.jivesoftware.os.amza.service.replication.PartitionComposter;
import com.jivesoftware.os.amza.service.replication.PartitionStatusStorage;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPIProvider;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.ring.RingNeighbors;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * Amza pronounced (AH m z ah )
 * Sanskrit word meaning partition / share.
 */
public class AmzaService implements AmzaInstance, AmzaPartitionAPIProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TimestampedOrderIdProvider orderIdProvider;
    private final AmzaStats amzaStats;
    private final AmzaRingReader ringReader;
    private final AmzaHostRing amzaHostRing;
    private final HighwaterStorage highwaterStorage;
    private final PartitionStatusStorage partitionStatusStorage;
    private final PartitionChangeTaker changeTaker;
    private final PartitionCompactor partitionCompactor;
    private final PartitionComposter partitionComposter;
    private final PartitionIndex partitionIndex;
    private final PartitionProvider partitionProvider;
    private final PartitionStripeProvider partitionStripeProvider;
    private final RecentPartitionTakers recentPartitionTakers;
    private final AmzaPartitionWatcher partitionWatcher;

    public AmzaService(TimestampedOrderIdProvider orderIdProvider,
        AmzaStats amzaStats,
        AmzaRingReader ringReader,
        AmzaHostRing amzaHostRing,
        HighwaterStorage highwaterMarks,
        PartitionStatusStorage partitionStatusStorage,
        PartitionChangeTaker changeTaker,
        PartitionCompactor partitionCompactor,
        PartitionComposter partitionComposter,
        PartitionIndex partitionIndex,
        PartitionProvider partitionProvider,
        PartitionStripeProvider partitionStripeProvider,
        RecentPartitionTakers recentPartitionTakers,
        AmzaPartitionWatcher partitionWatcher) {
        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.ringReader = ringReader;
        this.amzaHostRing = amzaHostRing;
        this.highwaterStorage = highwaterMarks;
        this.partitionStatusStorage = partitionStatusStorage;
        this.changeTaker = changeTaker;
        this.partitionCompactor = partitionCompactor;
        this.partitionComposter = partitionComposter;
        this.partitionIndex = partitionIndex;
        this.partitionProvider = partitionProvider;
        this.partitionStripeProvider = partitionStripeProvider;
        this.recentPartitionTakers = recentPartitionTakers;
        this.partitionWatcher = partitionWatcher;
    }

    public AmzaRingReader getAmzaRingReader() {
        return ringReader;
    }

    public AmzaHostRing getAmzaHostRing() {
        return amzaHostRing;
    }

    public HighwaterStorage getHighwaterMarks() {
        return highwaterStorage;
    }

    public PartitionStatusStorage getPartitionMemberStatusStorage() {
        return partitionStatusStorage;
    }

    public PartitionComposter getPartitionComposter() {
        return partitionComposter;
    }

    public PartitionProvider getPartitionProvider() {
        return partitionProvider;
    }

    synchronized public void start() throws Exception {
        changeTaker.start();
        partitionCompactor.start();
    }

    synchronized public void stop() throws Exception {
        changeTaker.stop();
        partitionCompactor.stop();
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

    public AmzaPartitionRoute getPartitionRoute(PartitionName partitionName) throws Exception {

        List<RingHost> orderedPartitionHosts = new ArrayList<>();
        List<RingMember> unregisteredRingMembers = new ArrayList<>();
        List<RingMember> ketchupRingMembers = new ArrayList<>();
        List<RingMember> expungedRingMembers = new ArrayList<>();
        List<RingMember> missingRingMembers = new ArrayList<>();

        NavigableMap<RingMember, RingHost> ring = amzaHostRing.getRing(partitionName.getRingName());

        PartitionProperties properties = partitionIndex.getProperties(partitionName);
        if (properties == null) {
            return new AmzaPartitionRoute(new ArrayList<>(ring.values()),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
        }
        if (amzaHostRing.isMemberOfRing(partitionName.getRingName())) {
            partitionStatusStorage.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
                if (partitionStatus == null) {
                    versionedPartitionName = partitionStatusStorage.markAsKetchup(partitionName);
                }

                partitionProvider.createPartitionStoreIfAbsent(versionedPartitionName, properties);
                return getPartition(partitionName);
            });
        }

        for (Entry<RingMember, RingHost> e : ring.entrySet()) {
            if (e.getValue() == RingHost.UNKNOWN_RING_HOST) {
                unregisteredRingMembers.add(e.getKey());
            }
            PartitionStatusStorage.VersionedStatus versionedStatus = partitionStatusStorage.getStatus(e.getKey(), partitionName);
            if (versionedStatus == null) {
                missingRingMembers.add(e.getKey());
            } else if (versionedStatus.status == TxPartitionStatus.Status.EXPUNGE) {
                expungedRingMembers.add(e.getKey());
            } else if (versionedStatus.status == TxPartitionStatus.Status.KETCHUP) {
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
    public AmzaPartition getPartition(PartitionName partitionName) throws Exception {
        return new AmzaPartition(amzaStats,
            orderIdProvider,
            ringReader.getRingMember(),
            partitionName,
            partitionStripeProvider.getPartitionStripe(partitionName),
            highwaterStorage,
            recentPartitionTakers);
    }

    public boolean hasPartition(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return true;
        } else {
            PartitionStore store = partitionIndex.get(PartitionProvider.REGION_INDEX);
            if (store != null) {
                byte[] rawPartitionName = partitionName.toBytes();
                WALValue timestampedKeyValueStoreName = store.get(new WALKey(rawPartitionName));
                if (timestampedKeyValueStoreName != null && !timestampedKeyValueStoreName.getTombstoned()) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public Set<PartitionName> getPartitionNames() {
        return Sets.newHashSet(Lists.newArrayList(Iterables.transform(partitionIndex.getAllPartitions(), (VersionedPartitionName input) -> {
            return input.getPartitionName();
        })));
    }

    public PartitionProperties getPartitionProperties(PartitionName partitionName) throws Exception {
        return partitionIndex.getProperties(partitionName);
    }

    @Override
    public void destroyPartition(final PartitionName partitionName) throws Exception {
        partitionProvider.destroyPartition(partitionName);
    }

    public void watch(PartitionName partitionName, RowChanges rowChanges) throws Exception {
        partitionWatcher.watch(partitionName, rowChanges);
    }

    public RowChanges unwatch(PartitionName partitionName) throws Exception {
        return partitionWatcher.unwatch(partitionName);
    }


    @Override
    public void streamingTakeFromPartition(DataOutputStream dos,
        RingMember ringMember,
        RingHost ringHost,
        PartitionName partitionName,
        long highestTransactionId) throws Exception {

        MutableLong bytes = new MutableLong(0);
        boolean needsToMarkAsKetchup = partitionStatusStorage.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            if (partitionStatus == TxPartitionStatus.Status.ONLINE) {
                dos.writeByte(1); // fully online
                bytes.increment();
                RingNeighbors hostRing = amzaHostRing.getRingNeighbors(partitionName.getRingName());
                for (Entry<RingMember, RingHost> node : hostRing.getAboveRing()) {
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
                AmzaPartition partition = getPartition(partitionName);
                if (partition != null) {
                    partition.takeRowUpdatesSince(highestTransactionId, (long rowFP, long rowTxId, RowType rowType, byte[] row) -> {
                        dos.writeByte(1);
                        dos.writeLong(rowTxId);
                        dos.writeByte(rowType.toByte());
                        dos.writeInt(row.length);
                        dos.write(row);
                        bytes.add(1 + 8 + 1 + 4 + row.length);
                        return true;
                    });
                }
                dos.writeByte(0); // last entry marker
                bytes.increment();
                recentPartitionTakers.took(ringMember, ringHost, partitionName);

            } else {
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
                }
            }
            return false;
        });

        amzaStats.netStats.wrote.addAndGet(bytes.longValue());

        if (needsToMarkAsKetchup) {
            try {
                if (amzaHostRing.isMemberOfRing(partitionName.getRingName()) && partitionProvider.hasPartition(partitionName)) {
                    partitionStatusStorage.markAsKetchup(partitionName);
                }
            } catch (Exception x) {
                LOG.warn("Failed to mark as ketchup for partition {}", new Object[] { partitionName }, x);
            }
        }
    }

}
