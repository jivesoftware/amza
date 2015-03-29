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
package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RegionProvider implements RowChanges {

    public static final RegionName RING_INDEX = new RegionName(true, "system", "RING_INDEX");
    public static final RegionName REGION_INDEX = new RegionName(true, "system", "REGION_INDEX");
    public static final RegionName REGION_PROPERTIES = new RegionName(true, "system", "REGION_PROPERTIES");
    public static final RegionName HIGHWATER_MARK_INDEX = new RegionName(true, "system", "HIGHWATER_MARKS");

    private final AmzaStats amzaStats;
    private final OrderIdProvider orderIdProvider;
    private final RegionPropertyMarshaller regionPropertyMarshaller;
    private final String[] workingDirectories;
    private final String domain;
    private final WALStorageProvider walStorageProvider;
    private final RowChanges rowChanges;
    private final WALReplicator walReplicator;
    private final ConcurrentHashMap<RegionName, RegionStore> regionStores = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RegionName, RegionProperties> regionProperties = new ConcurrentHashMap<>();
    private final StripingLocksProvider locksProvider = new StripingLocksProvider(1024); // TODO expose to config

    public RegionProvider(AmzaStats amzaStats,
        OrderIdProvider orderIdProvider,
        RegionPropertyMarshaller regionPropertyMarshaller,
        String[] workingDirectories,
        String domain,
        WALStorageProvider walStorageProvider,
        RowChanges rowChanges,
        WALReplicator rowReplicator) {

        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.regionPropertyMarshaller = regionPropertyMarshaller;
        this.workingDirectories = workingDirectories;
        this.domain = domain;
        this.walStorageProvider = walStorageProvider;
        this.rowChanges = rowChanges;
        this.walReplicator = rowReplicator;
    }

    public String getName() {
        return domain;
    }

    public void open() throws Exception {
        RegionStore regionIndexStore = getRegionIndexStore();
        regionIndexStore.rowScan(new WALScan() {

            @Override
            public boolean row(long rowTxId, WALKey key, WALValue value) throws Exception {
                RegionName regionName = RegionName.fromBytes(key.getKey());
                RegionProperties properties = getRegionProperties(regionName);
                if (properties != null) {
                    getRegionStore(regionName);
                }
                return true;
            }
        });
    }

    public RegionStore getRingIndexStore() throws Exception {
        return getRegionStore(RING_INDEX);
    }

    public RegionStore getHighwaterIndexStore() throws Exception {
        return getRegionStore(HIGHWATER_MARK_INDEX);
    }

    private RegionStore getRegionPropertiesStore() throws Exception {
        return getRegionStore(REGION_PROPERTIES);
    }

    public RegionStore createRegionStoreIfAbsent(RegionName regionName, RegionProperties properties) throws Exception {
        RegionStore regionStore = regionStores.get(regionName);
        if (regionStore != null) {
            return regionStore;
        }
        RegionProperties existing = getRegionProperties(regionName);
        if (existing == null) {
            if (regionName.isSystemRegion()) {
                regionProperties.put(regionName, properties);
            } else {
                setRegionProperties(regionName, properties);
            }
        }
        return getRegionStore(regionName);
    }

    public RegionStore getRegionStore(RegionName regionName) throws Exception {
        RegionStore regionStore = regionStores.get(regionName);
        if (regionStore != null) {
            return regionStore;
        }
        RegionProperties properties;
        if (regionName.isSystemRegion()) {
            properties = coldstartSystemRegionProperties(regionName);
        } else {
            properties = getRegionProperties(regionName);
            if (properties == null) {
                return null;
            }
        }

        synchronized (locksProvider.lock(regionName, 1234)) {
            regionStore = regionStores.get(regionName);
            if (regionStore != null) {
                return regionStore;
            }

            File workingDirectory = new File(workingDirectories[Math.abs(regionName.hashCode()) % workingDirectories.length]);
            WALStorage walStorage = walStorageProvider.create(workingDirectory, domain, regionName, properties.walStorageDescriptor, walReplicator);
            regionStore = new RegionStore(amzaStats, regionName, walStorage, rowChanges);
            regionStore.load();

            regionStores.put(regionName, regionStore);
            RowStoreUpdates tx;
            if (!regionName.equals(REGION_INDEX)) {
                RegionStore regionIndexStore = getRegionIndexStore();
                tx = regionIndexStore.startTransaction(orderIdProvider.nextId());
            } else {
                tx = regionStore.startTransaction(orderIdProvider.nextId());
            }
            byte[] rawRegionName = regionName.toBytes();
            tx.add(new WALKey(rawRegionName), rawRegionName);
            tx.commit();
            return regionStore;
        }
    }

    public RegionStore getRegionIndexStore() throws Exception {
        return getRegionStore(REGION_INDEX);
    }

    public RegionProperties getRegionProperties(RegionName regionName) throws Exception {
        RegionProperties properties = regionProperties.get(regionName);
        if (properties != null) {
            return properties;
        }
        if (regionName.isSystemRegion()) {
            return coldstartSystemRegionProperties(regionName);
        }
        RegionStore regionPropertiesStore = getRegionPropertiesStore();
        WALValue rawRegionProperties = regionPropertiesStore.get(new WALKey(regionName.toBytes()));
        if (rawRegionProperties == null || rawRegionProperties.getTombstoned()) {
            return null;
        }
        properties = regionPropertyMarshaller.fromBytes(rawRegionProperties.getValue());
        regionProperties.put(regionName, properties);
        return properties;
    }

    public void setRegionProperties(RegionName regionName, RegionProperties properties) throws Exception {
        regionProperties.put(regionName, properties);
        RegionStore regionPropertiesStore = getRegionPropertiesStore();
        RowStoreUpdates rsu = regionPropertiesStore.startTransaction(orderIdProvider.nextId());
        rsu.add(new WALKey(regionName.toBytes()), regionPropertyMarshaller.toBytes(properties));
        rsu.commit();
    }

    public Set<Map.Entry<RegionName, RegionStore>> getAll() {
        return regionStores.entrySet();
    }

    public Set<RegionName> getActiveRegions() {
        return regionStores.keySet();
    }

    private RegionProperties coldstartSystemRegionProperties(RegionName regionName) {
        RegionProperties properties;
        if (regionName.equals(HIGHWATER_MARK_INDEX)) {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                new PrimaryIndexDescriptor("memory", Long.MAX_VALUE, false, null), null, 1000, 1000);
            properties = new RegionProperties(storageDescriptor, 0, 0, false);
        } else {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                new PrimaryIndexDescriptor("memory", Long.MAX_VALUE, false, null), null, 1000, 1000);
            properties = new RegionProperties(storageDescriptor, 2, 2, false);
        }
        regionProperties.put(regionName, properties);
        return properties;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (changes.getRegionName().equals(REGION_PROPERTIES)) {
            //TODO add metrics
            for (WALKey key : changes.getApply().keySet()) {
                regionProperties.remove(RegionName.fromBytes(key.getKey()));
                RegionStore store = regionStores.get(changes.getRegionName());
                if (store != null) {
                    RegionProperties properties = getRegionProperties(changes.getRegionName());
                    store.updatedStorageDescriptor(properties.walStorageDescriptor);
                }
            }
        }
    }

}
