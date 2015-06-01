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
import com.jivesoftware.os.amza.service.replication.RegionChangeReceiver;
import com.jivesoftware.os.amza.service.replication.RegionChangeReplicator;
import com.jivesoftware.os.amza.service.replication.RegionChangeTaker;
import com.jivesoftware.os.amza.service.replication.RegionCompactor;
import com.jivesoftware.os.amza.service.replication.RegionComposter;
import com.jivesoftware.os.amza.service.replication.RegionStatusStorage;
import com.jivesoftware.os.amza.service.replication.RegionStripeProvider;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.Commitable;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.RingNeighbors;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowType;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.TakeCursors;
import com.jivesoftware.os.amza.shared.TakeCursors.RingMemberCursor;
import com.jivesoftware.os.amza.shared.TxRegionStatus;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * Amza pronounced (AH m z ah )
 * Sanskrit word meaning partition / share.
 */
public class AmzaService implements AmzaInstance {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TimestampedOrderIdProvider orderIdProvider;
    private final AmzaStats amzaStats;
    private final AmzaRingReader ringReader;
    private final AmzaHostRing amzaHostRing;
    private final HighwaterStorage highwaterStorage;
    private final RegionStatusStorage regionStatusStorage;
    private final RegionChangeReceiver changeReceiver;
    private final RegionChangeTaker changeTaker;
    private final RegionChangeReplicator changeReplicator;
    private final RegionCompactor regionCompactor;
    private final RegionComposter regionComposter;
    private final RegionIndex regionIndex;
    private final RegionProvider regionProvider;
    private final RegionStripeProvider regionStripeProvider;
    private final WALReplicator replicator;
    private final AmzaRegionWatcher regionWatcher;

    public AmzaService(TimestampedOrderIdProvider orderIdProvider,
        AmzaStats amzaStats,
        AmzaRingReader ringReader,
        AmzaHostRing amzaHostRing,
        HighwaterStorage highwaterMarks,
        RegionStatusStorage regionStatusStorage,
        RegionChangeReceiver changeReceiver,
        RegionChangeTaker changeTaker,
        RegionChangeReplicator changeReplicator,
        RegionCompactor regionCompactor,
        RegionComposter regionComposter,
        RegionIndex regionIndex,
        RegionProvider regionProvider,
        RegionStripeProvider regionStripeProvider,
        WALReplicator replicator,
        AmzaRegionWatcher regionWatcher) {
        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.ringReader = ringReader;
        this.amzaHostRing = amzaHostRing;
        this.highwaterStorage = highwaterMarks;
        this.regionStatusStorage = regionStatusStorage;
        this.changeReceiver = changeReceiver;
        this.changeTaker = changeTaker;
        this.changeReplicator = changeReplicator;
        this.regionCompactor = regionCompactor;
        this.regionComposter = regionComposter;
        this.regionIndex = regionIndex;
        this.regionProvider = regionProvider;
        this.regionStripeProvider = regionStripeProvider;
        this.replicator = replicator;
        this.regionWatcher = regionWatcher;
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

    public RegionStatusStorage getRegionMemberStatusStorage() {
        return regionStatusStorage;
    }

    public RegionComposter getRegionComposter() {
        return regionComposter;
    }

    public RegionProvider getRegionProvider() {
        return regionProvider;
    }

    synchronized public void start() throws Exception {
        changeReceiver.start();
        changeTaker.start();
        changeReplicator.start();
        regionCompactor.start();
    }

    synchronized public void stop() throws Exception {
        changeReceiver.stop();
        changeTaker.stop();
        changeReplicator.stop();
        regionCompactor.stop();
    }

    @Override
    public long getTimestamp(long timestampId, long deltaMillis) throws Exception {
        if (timestampId <= 0) {
            return 0;
        }
        return orderIdProvider.getApproximateId(timestampId, deltaMillis);
    }

    public AmzaRegion createRegionIfAbsent(RegionName regionName, RegionProperties regionProperties) throws Exception {
        if (amzaHostRing.isMemberOfRing(regionName.getRingName())) {
            return regionStatusStorage.tx(regionName, (versionedRegionName, regionStatus) -> {
                if (regionStatus == null) {
                    versionedRegionName = regionStatusStorage.markAsKetchup(regionName);
                }

                regionProvider.createRegionStoreIfAbsent(versionedRegionName, regionProperties);
                return getRegion(regionName);
            });
        } else {
            throw new IllegalStateException(amzaHostRing.getRingMember() + " is not a member for regionName:" + regionName);
        }

    }

    public AmzaRegion getRegion(RegionName regionName) throws Exception {
        return new AmzaRegion(amzaStats, orderIdProvider, regionName, replicator, regionStripeProvider.getRegionStripe(regionName), highwaterStorage);
    }

    public boolean hasRegion(RegionName regionName) throws Exception {
        if (regionName.isSystemRegion()) {
            return true;
        } else {
            RegionStore store = regionIndex.get(RegionProvider.REGION_INDEX);
            if (store != null) {
                byte[] rawRegionName = regionName.toBytes();
                WALValue timestampedKeyValueStoreName = store.get(new WALKey(rawRegionName));
                if (timestampedKeyValueStoreName != null && !timestampedKeyValueStoreName.getTombstoned()) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public Set<RegionName> getRegionNames() {
        return Sets.newHashSet(Lists.newArrayList(Iterables.transform(regionIndex.getAllRegions(), (VersionedRegionName input) -> {
            return input.getRegionName();
        })));
    }

    public RegionProperties getRegionProperties(RegionName regionName) throws Exception {
        return regionIndex.getProperties(regionName);
    }

    @Override
    public void destroyRegion(final RegionName regionName) throws Exception {
        regionProvider.destroyRegion(regionName);
    }

    @Override
    public void updates(RegionName regionName, Commitable<WALValue> rowUpdates) throws Exception {
        changeReceiver.receiveChanges(regionName, rowUpdates);
    }

    public void watch(RegionName regionName, RowChanges rowChanges) throws Exception {
        regionWatcher.watch(regionName, rowChanges);
    }

    public RowChanges unwatch(RegionName regionName) throws Exception {
        return regionWatcher.unwatch(regionName);
    }

    public boolean replicate(RegionName regionName, Commitable<WALValue> rowUpdates, int requireNReplicas) throws Exception {
        NavigableMap<RingMember, RingHost> nodes = ringReader.getRing(regionName.getRingName());
        //TODO consider spinning until we reach quorum, or force election to the sub-ring
        RegionProperties regionProperties = getRegionProperties(regionName);
        int numReplicated = changeReplicator.replicateUpdatesToRingHosts(regionName,
            rowUpdates,
            false,
            nodes.entrySet().toArray(new Entry[nodes.size()]),
            regionProperties.replicationFactor);
        return numReplicated >= requireNReplicas;
    }

    private static final BiFunction<Long, Long, Long> maxMerge = (Long t, Long u) -> Math.max(t, u);

    public TakeCursors takeFromTransactionId(AmzaRegion region, long transactionId, final Scan<WALValue> scan)
        throws Exception {
        if (region == null) {
            return null;
        }

        Map<RingMember, Long> ringMemberToMaxTxId = new HashMap<>();
        AmzaRegion.TakeResult takeResult = region.takeFromTransactionId(transactionId, (highwater) -> {
            for (RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                ringMemberToMaxTxId.merge(memberHighwater.ringMember, memberHighwater.transactionId, maxMerge);
            }
        }, scan);
        if (takeResult.tookToEnd != null) {
            for (RingMemberHighwater highwater : takeResult.tookToEnd.ringMemberHighwater) {
                ringMemberToMaxTxId.merge(highwater.ringMember, highwater.transactionId, maxMerge);
            }
        }
        ringMemberToMaxTxId.merge(amzaHostRing.getRingMember(), takeResult.lastTxId, maxMerge);

        List<RingMemberCursor> cursors = new ArrayList<>();
        for (Entry<RingMember, Long> entry : ringMemberToMaxTxId.entrySet()) {
            cursors.add(new RingMemberCursor(entry.getKey(), entry.getValue()));
        }
        cursors.add(new TakeCursors.RingMemberCursor(amzaHostRing.getRingMember(), takeResult.lastTxId));
        return new TakeCursors(cursors);

    }

    @Override
    public void streamingTakeFromRegion(DataOutputStream dos,
        RegionName regionName,
        long highestTransactionId) throws Exception {

        MutableLong bytes = new MutableLong(0);
        boolean needsToMarkAsKetchup = regionStatusStorage.tx(regionName, (versionedRegionName, regionStatus) -> {
            if (regionStatus == TxRegionStatus.Status.ONLINE) {
                dos.writeByte(1); // fully online
                bytes.increment();
                RingNeighbors hostRing = amzaHostRing.getRingNeighbors(regionName.getRingName());
                for (Entry<RingMember, RingHost> node : hostRing.getAboveRing()) {
                    Long highwatermark = highwaterStorage.get(node.getKey(), versionedRegionName);
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
                AmzaRegion region = getRegion(regionName);
                if (region != null) {
                    region.takeRowUpdatesSince(highestTransactionId, (long rowFP, long rowTxId, RowType rowType, byte[] row) -> {
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
            } else {
                dos.writeByte(0); // not online
                dos.writeByte(0); // last entry marker
                dos.writeByte(0); // last entry marker
                bytes.add(3);

                if (versionedRegionName == null || regionStatus == null) {
                    // someone thinks we're a member for this region
                    return true;
                } else {
                    // BOOTSTRAP'S BOOTSTRAPS!
                    regionIndex.get(versionedRegionName);
                }
            }
            return false;
        });

        amzaStats.netStats.wrote.addAndGet(bytes.longValue());

        if (needsToMarkAsKetchup) {
            try {
                if (amzaHostRing.isMemberOfRing(regionName.getRingName()) && regionProvider.hasRegion(regionName)) {
                    regionStatusStorage.markAsKetchup(regionName);
                }
            } catch (Exception x) {
                LOG.warn("Failed to mark as ketchup for region {}", new Object[] { regionName }, x);
            }
        }
    }

}
