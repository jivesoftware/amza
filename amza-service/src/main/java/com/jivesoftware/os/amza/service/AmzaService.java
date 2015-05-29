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
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.TakeCursors;
import com.jivesoftware.os.amza.shared.TakeCursors.RingMemberCursor;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Amza pronounced (AH m z ah )
 * Sanskrit word meaning partition / share.
 */
public class AmzaService implements AmzaInstance {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TimestampedOrderIdProvider orderIdProvider;
    private final AmzaStats amzaStats;
    private final AmzaRingReader ringReader;
    private final AmzaHostRing amzaRing;
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
        AmzaHostRing amzaRing,
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
        this.amzaRing = amzaRing;
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

    public AmzaHostRing getAmzaRing() {
        return amzaRing;
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
        if (amzaRing.isMemberOfRing(regionName.getRingName())) {
            return regionStatusStorage.tx(regionName, (versionedRegionName, regionStatus) -> {
                if (regionStatus == null) {
                    versionedRegionName = regionStatusStorage.markAsKetchup(regionName);
                }

                regionProvider.createRegionStoreIfAbsent(versionedRegionName, regionProperties);
                return getRegion(regionName);
            });
        } else {
            throw new IllegalStateException(amzaRing.getRingMember() + " is not a member for regionName:" + regionName);
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
        RegionStore regionIndexStore = regionIndex.get(RegionProvider.REGION_INDEX);
        regionIndexStore.directCommit(false, replicator, WALStorageUpdateMode.replicateThenUpdate, (highwaters, scan) -> {
            scan.row(-1, new WALKey(regionName.toBytes()), new WALValue(null, orderIdProvider.nextId(), true));
        });
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
            (Entry<RingMember, RingHost>[]) nodes.entrySet().toArray(new Entry[nodes.size()]),
            regionProperties.replicationFactor);
        return numReplicated >= requireNReplicas;
    }

    @Override
    public void takeRowUpdates(RegionName regionName, long transactionId, RowStream rowStream) throws Exception {
        AmzaRegion region = getRegion(regionName);
        if (region != null) {
            region.takeRowUpdatesSince(transactionId, rowStream);
        }
    }

    private static final BiFunction<Long, Long, Long> maxMerge = (Long t, Long u) -> {
        return Math.max(t, u);
    };

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
        ringMemberToMaxTxId.merge(amzaRing.getRingMember(), takeResult.lastTxId, maxMerge);

        List<RingMemberCursor> cursors = new ArrayList<>();
        for (Entry<RingMember, Long> entry : ringMemberToMaxTxId.entrySet()) {
            cursors.add(new RingMemberCursor(entry.getKey(), entry.getValue()));
        }
        cursors.add(new TakeCursors.RingMemberCursor(amzaRing.getRingMember(), takeResult.lastTxId));
        return new TakeCursors(cursors);

    }

}
