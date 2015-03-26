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

import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeReceiver;
import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeReplicator;
import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeTaker;
import com.jivesoftware.os.amza.service.replication.AmzaRegionCompactor;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.service.storage.RowStoreUpdates;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Amza pronounced (AH m z ah )
 * <p/>
 * Sanskrit word meaning partition / share.
 */
public class AmzaService implements AmzaInstance {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TimestampedOrderIdProvider orderIdProvider;
    private final AmzaHostRing amzaRing;
    private final AmzaRegionChangeReceiver changeReceiver;
    private final AmzaRegionChangeTaker changeTaker;
    private final AmzaRegionChangeReplicator changeReplicator;
    private final AmzaRegionCompactor regionCompactor;
    private final AmzaRegionWatcher regionWatcher;
    private final RegionProvider regionProvider;
    private final RegionName regionIndexKey = new RegionName("MASTER", "REGION_INDEX", null, null);

    public AmzaService(TimestampedOrderIdProvider orderIdProvider,
        AmzaHostRing amzaRing,
        AmzaRegionChangeReceiver changeReceiver,
        AmzaRegionChangeTaker changeTaker,
        AmzaRegionChangeReplicator changeReplicator,
        AmzaRegionCompactor regionCompactor,
        RegionProvider regionProvider,
        AmzaRegionWatcher regionWatcher) {
        this.orderIdProvider = orderIdProvider;
        this.amzaRing = amzaRing;
        this.changeReceiver = changeReceiver;
        this.changeTaker = changeTaker;
        this.changeReplicator = changeReplicator;
        this.regionCompactor = regionCompactor;
        this.regionProvider = regionProvider;
        this.regionWatcher = regionWatcher;
    }

    public AmzaHostRing getAmzaRing() {
        return amzaRing;
    }

    public RegionProvider getRegionProvider() {
        return regionProvider;
    }

    synchronized public void start() throws Exception {
        changeReceiver.start();
        changeTaker.start();
        changeReplicator.start();
        regionCompactor.start();
        changeTaker.takeChanges(); // HACK
    }

    synchronized public void stop() throws Exception {
        changeReceiver.stop();
        changeTaker.stop();
        changeReplicator.stop();
        regionCompactor.stop();
    }

    @Override
    public long getTimestamp(long timestampId, long wallClockMillis) throws Exception {
        return orderIdProvider.getApproximateId(timestampId, wallClockMillis);
    }

    private boolean createRegion(RegionName regionName) throws Exception {
        byte[] rawRegionName = regionName.toBytes();

        RegionStore regionNameIndex = regionProvider.get(regionIndexKey);
        WALValue timestamptedRegionKey = regionNameIndex.get(new WALKey(rawRegionName));
        if (timestamptedRegionKey == null) {
            RowStoreUpdates tx = regionNameIndex.startTransaction(orderIdProvider.nextId());
            tx.add(new WALKey(rawRegionName), rawRegionName);
            tx.commit();
            return true;
        } else {
            return !timestamptedRegionKey.getTombstoned();
        }
    }

    public AmzaRegion getRegion(RegionName regionName) throws Exception {
        byte[] rawRegionName = regionName.toBytes();
        RegionStore regionStoreIndex = regionProvider.get(regionIndexKey);
        WALValue timestampedKeyValueStoreName = regionStoreIndex.get(new WALKey(rawRegionName));
        while (timestampedKeyValueStoreName == null) {
            createRegion(regionName);
            timestampedKeyValueStoreName = regionStoreIndex.get(new WALKey(rawRegionName));
        }
        if (timestampedKeyValueStoreName.getTombstoned()) {
            return null;
        } else {
            RegionStore regionStore = regionProvider.get(regionName);
            return new AmzaRegion(orderIdProvider, regionName, regionStore);
        }
    }

    @Override
    public List<RegionName> getRegionNames() {
        List<RegionName> regionNames = new ArrayList<>();
        for (Entry<RegionName, RegionStore> regionStore : regionProvider.getAll()) {
            regionNames.add(regionStore.getKey());
        }
        return regionNames;
    }

    public Map<RegionName, AmzaRegion> getRegions() throws Exception {
        Map<RegionName, AmzaRegion> regions = new HashMap<>();
        for (Entry<RegionName, RegionStore> regionStore : regionProvider.getAll()) {
            regions.put(regionStore.getKey(), new AmzaRegion(orderIdProvider, regionStore.getKey(), regionStore.getValue()));
        }
        return regions;
    }

    @Override
    public void destroyRegion(RegionName regionName) throws Exception {
        byte[] rawRegionName = regionName.toBytes();
        RegionStore regionStore = regionProvider.get(regionIndexKey);
        RowStoreUpdates tx = regionStore.startTransaction(orderIdProvider.nextId());
        tx.remove(new WALKey(rawRegionName));
        tx.commit();
    }

    @Override
    public void updates(RegionName regionName, WALScanable rowUpdates) throws Exception {
        changeReceiver.receiveChanges(regionName, rowUpdates);
    }

    public void watch(RegionName regionName, RowChanges rowChanges) throws Exception {
        regionWatcher.watch(regionName, rowChanges);
    }

    public RowChanges unwatch(RegionName regionName) throws Exception {
        return regionWatcher.unwatch(regionName);
    }

    public boolean replicate(RegionName regionName, WALScanable rowUpdates) throws Exception {
        AmzaHostRing amzaRing = getAmzaRing();
        List<RingHost> ringHosts = amzaRing.getRing(regionName.getRingName());
        if (ringHosts.contains(amzaRing.getRingHost())) {
            regionProvider.get(regionName).commit(rowUpdates);
            return true;
        } else {
            return changeReplicator.replicateUpdatesToRingHosts(regionName, rowUpdates, false, ringHosts.toArray(new RingHost[ringHosts.size()]));
        }
    }

    @Override
    public void takeRowUpdates(RegionName regionName, long transationId, WALScan rowUpdates) throws Exception {
        getRegion(regionName).takeRowUpdatesSince(transationId, rowUpdates);
    }

    //------ Used for debugging ------
    public void printService(final RingHost ringHost) throws Exception {
        for (Map.Entry<RegionName, RegionStore> region : regionProvider.getAll()) {
//            final RegionName tableName = table.getKey();
//            final RegionStore sortedMapStore = table.getValue();
//            sortedMapStore.rowScan(new WALScan<RuntimeException>() {
//
//                @Override
//                public boolean row(long orderId, WALKey key, WALValue value) throws RuntimeException {
//                    System.out.println("INDEX:"
//                        + tableName.getTableName() + " k:" + key
//                        + " d:" + value.getTombstoned() + " t:" + value.getTimestampId()
//                        + " v:" + Arrays.toString(value.getValue())
//                        + ringHost.getHost() + ":" + ringHost.getPort());
//                    return true;
//                }
//            });

//            sortedMapStore.takeRowUpdatesSince(0, new WALScan<RuntimeException>() {
//
//                @Override
//                public boolean row(long orderId, WALKey key, WALValue value) throws RuntimeException {
//                    System.out.println("WAL:"
//                        + tableName.getTableName() + " k:" + BaseEncoding.base64().encode(key.getKey())
//                        + " d:" + value.getTombstoned() + " t:" + value.getTimestampId()
//                        + " v:" + BaseEncoding.base64().encode(value.getValue())
//                        + ringHost.getHost() + ":" + ringHost.getPort()
//                    );
//                    return true;
//                }
//            });
        }
    }
}
