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
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeReceiver;
import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeReplicator;
import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeTaker;
import com.jivesoftware.os.amza.service.replication.AmzaRegionCompactor;
import com.jivesoftware.os.amza.service.replication.RegionStripe;
import com.jivesoftware.os.amza.service.replication.RegionStripeProvider;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Amza pronounced (AH m z ah )
 * <p/>
 * Sanskrit word meaning partition / share.
 */
public class AmzaService implements AmzaInstance {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TimestampedOrderIdProvider orderIdProvider;
    private final AmzaStats amzaStats;
    private final AmzaRingReader ringReader;
    private final AmzaHostRing amzaRing;
    private final HighwaterMarks highwaterMarks;
    private final AmzaRegionChangeReceiver changeReceiver;
    private final AmzaRegionChangeTaker changeTaker;
    private final AmzaRegionChangeReplicator changeReplicator;
    private final AmzaRegionCompactor regionCompactor;
    private final RegionIndex regionIndex;
    private final RegionProvider regionProvider;
    private final RegionStripeProvider regionStripeProvider;
    private final WALReplicator replicator;
    private final AmzaRegionWatcher regionWatcher;

    public AmzaService(TimestampedOrderIdProvider orderIdProvider,
        AmzaStats amzaStats,
        AmzaRingReader ringReader,
        AmzaHostRing amzaRing,
        HighwaterMarks highwaterMarks,
        AmzaRegionChangeReceiver changeReceiver,
        AmzaRegionChangeTaker changeTaker,
        AmzaRegionChangeReplicator changeReplicator,
        AmzaRegionCompactor regionCompactor,
        RegionIndex regionIndex,
        RegionProvider regionProvider,
        RegionStripeProvider regionStripeProvider,
        WALReplicator replicator,
        AmzaRegionWatcher regionWatcher) {
        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.ringReader = ringReader;
        this.amzaRing = amzaRing;
        this.highwaterMarks = highwaterMarks;
        this.changeReceiver = changeReceiver;
        this.changeTaker = changeTaker;
        this.changeReplicator = changeReplicator;
        this.regionCompactor = regionCompactor;
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

    public HighwaterMarks getHighwaterMarks() {
        return highwaterMarks;
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
        regionProvider.createRegionStoreIfAbsent(regionName, regionProperties);
        Optional<RegionStripe> regionStripe = regionStripeProvider.getRegionStripe(regionName);
        if (regionStripe.isPresent()) {
            return new AmzaRegion(amzaStats, orderIdProvider, regionName, replicator, regionStripe.get());
        } else {
            throw new IllegalStateException("Coding is hard. Should never get here.");
        }
    }

    public AmzaRegion getRegion(RegionName regionName) throws Exception {
        if (regionName.isSystemRegion()) {
            return new AmzaRegion(amzaStats, orderIdProvider, regionName, replicator, regionStripeProvider.getSystemRegionStripe());
        } else {
            RegionStore store = regionIndex.get(RegionProvider.REGION_INDEX);
            if (store != null) {
                byte[] rawRegionName = regionName.toBytes();
                WALValue timestampedKeyValueStoreName = store.get(new WALKey(rawRegionName));
                if (timestampedKeyValueStoreName != null && !timestampedKeyValueStoreName.getTombstoned()) {
                    Optional<RegionStripe> regionStripe = regionStripeProvider.getRegionStripe(regionName);
                    if (regionStripe.isPresent()) {
                        return new AmzaRegion(amzaStats, orderIdProvider, regionName, replicator, regionStripe.get());
                    }
                }
            }
        }
        return null;
    }

    @Override
    public List<RegionName> getRegionNames() {
        return Lists.newArrayList(regionIndex.getActiveRegions());
    }

    public RegionProperties getRegionProperties(RegionName regionName) throws Exception {
        return regionIndex.getProperties(regionName);
    }

    public Map<RegionName, AmzaRegion> getRegions() throws Exception {
        Map<RegionName, AmzaRegion> regions = new HashMap<>();
        for (RegionName regionName : regionIndex.getActiveRegions()) {
            Optional<RegionStripe> regionStripe = regionStripeProvider.getRegionStripe(regionName);
            if (regionStripe.isPresent()) {
                regions.put(regionName, new AmzaRegion(amzaStats, orderIdProvider, regionName, replicator, regionStripe.get()));
            } else {
                LOG.warn("{} is not yet available.", regionName);
            }
        }
        return regions;
    }

    @Override
    public void destroyRegion(final RegionName regionName) throws Exception {
        RegionStore regionIndexStore = regionIndex.get(RegionProvider.REGION_INDEX);
        regionIndexStore.directCommit(false, replicator, WALStorageUpdateMode.replicateThenUpdate, new Scannable<WALValue>() {

            @Override
            public void rowScan(Scan<WALValue> scan) throws Exception {
                scan.row(-1, new WALKey(regionName.toBytes()), new WALValue(null, orderIdProvider.nextId(), true));
            }
        });
    }

    @Override
    public void updates(RegionName regionName, Scannable<WALValue> rowUpdates) throws Exception {
        changeReceiver.receiveChanges(regionName, rowUpdates);
    }

    public void watch(RegionName regionName, RowChanges rowChanges) throws Exception {
        regionWatcher.watch(regionName, rowChanges);
    }

    public RowChanges unwatch(RegionName regionName) throws Exception {
        return regionWatcher.unwatch(regionName);
    }

    public boolean replicate(RegionName regionName, Scannable<WALValue> rowUpdates, int replicateToNHosts, int requireNReplicas) throws Exception {
        List<RingHost> ringHosts = ringReader.getRing(regionName.getRingName());
        //TODO consider spinning until we reach quorum, or force election to the sub-ring
        int numReplicated = changeReplicator.replicateUpdatesToRingHosts(regionName,
            rowUpdates,
            false,
            ringHosts.toArray(new RingHost[ringHosts.size()]),
            replicateToNHosts);
        return numReplicated >= requireNReplicas;
    }

    @Override
    public void takeRowUpdates(RegionName regionName, long transactionId, RowStream rowStream) throws Exception {
        AmzaRegion region = getRegion(regionName);
        if (region != null) {
            region.takeRowUpdatesSince(transactionId, rowStream);
        }
    }

    @Override
    public void takeFromTransactionId(RegionName regionName, long transactionId, Scan<WALValue> scan) throws Exception {
        AmzaRegion region = getRegion(regionName);
        if (region != null) {
            region.takeFromTransactionId(transactionId, scan);
        }
    }

}
