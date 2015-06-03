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

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.shared.region.RegionName;
import com.jivesoftware.os.amza.shared.region.RegionProperties;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.region.VersionedRegionName;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

public class RegionProvider {

    public static final VersionedRegionName NODE_INDEX = new VersionedRegionName(new RegionName(true, "system", "NODE_INDEX"), 0);
    public static final VersionedRegionName RING_INDEX = new VersionedRegionName(new RegionName(true, "system", "RING_INDEX"), 0);
    public static final VersionedRegionName REGION_INDEX = new VersionedRegionName(new RegionName(true, "system", "REGION_INDEX"), 0);
    public static final VersionedRegionName REGION_PROPERTIES = new VersionedRegionName(new RegionName(true, "system", "REGION_PROPERTIES"), 0);
    public static final VersionedRegionName HIGHWATER_MARK_INDEX = new VersionedRegionName(new RegionName(true, "system", "HIGHWATER_MARK_INDEX"), 0);
    public static final VersionedRegionName REGION_ONLINE_INDEX = new VersionedRegionName(new RegionName(true, "system", "REGION_ONLINE_INDEX"), 0);

    private final OrderIdProvider orderIdProvider;
    private final RegionPropertyMarshaller regionPropertyMarshaller;
    private final RegionIndex regionIndex;
    private final RowChanges rowChanges;
    private final boolean hardFlush;

    public RegionProvider(OrderIdProvider orderIdProvider,
        RegionPropertyMarshaller regionPropertyMarshaller,
        RegionIndex regionIndex,
        RowChanges rowChanges,
        boolean hardFlush) {

        this.orderIdProvider = orderIdProvider;
        this.regionPropertyMarshaller = regionPropertyMarshaller;
        this.regionIndex = regionIndex;
        this.rowChanges = rowChanges;
        this.hardFlush = hardFlush;
    }

    public boolean hasRegion(RegionName regionName) throws Exception {
        if (regionName.isSystemRegion()) {
            return true;
        }

        final byte[] rawRegionName = regionName.toBytes();
        final WALKey regionKey = new WALKey(rawRegionName);
        RegionStore regionPropertiesStore = regionIndex.get(REGION_PROPERTIES);
        WALValue propertiesValue = regionPropertiesStore.get(regionKey);
        if (propertiesValue != null && !propertiesValue.getTombstoned()) {
            RegionStore regionIndexStore = regionIndex.get(REGION_INDEX);
            WALValue indexValue = regionIndexStore.get(regionKey);
            if (indexValue != null && !indexValue.getTombstoned()) {
                return true;
            }
        }
        return false;
    }

    public void createRegionStoreIfAbsent(VersionedRegionName versionedRegionName, RegionProperties properties) throws Exception {
        RegionName regionName = versionedRegionName.getRegionName();
        Preconditions.checkArgument(!regionName.isSystemRegion(), "You cannot create a system region");

        RegionStore regionStore = regionIndex.get(versionedRegionName);
        if (regionStore == null) {
            updateRegionProperties(regionName, properties);

            byte[] rawRegionName = regionName.toBytes();
            WALKey regionKey = new WALKey(rawRegionName);
            RegionStore regionIndexStore = regionIndex.get(REGION_INDEX);
            RowsChanged changed = regionIndexStore.directCommit(false, (highwater, scan) -> {
                scan.row(-1, regionKey, new WALValue(rawRegionName, orderIdProvider.nextId(), false));
            });
            regionIndexStore.flush(hardFlush);
            if (!changed.isEmpty()) {
                rowChanges.changes(changed);
            }
        }
    }

    public void updateRegionProperties(RegionName regionName, RegionProperties properties) throws Exception {
        RegionStore regionPropertiesStore = regionIndex.get(REGION_PROPERTIES);
        RowsChanged changed = regionPropertiesStore.directCommit(false, (highwater, scan) -> {
            scan.row(-1, new WALKey(regionName.toBytes()), new WALValue(regionPropertyMarshaller.toBytes(properties), orderIdProvider.nextId(), false));
        });
        regionPropertiesStore.flush(hardFlush);
        if (!changed.isEmpty()) {
            rowChanges.changes(changed);
        }
        regionIndex.putProperties(regionName, properties);
    }

    public void destroyRegion(RegionName regionName) throws Exception {
        Preconditions.checkArgument(!regionName.isSystemRegion(), "You cannot destroy a system region");

        RegionStore regionIndexStore = regionIndex.get(RegionProvider.REGION_INDEX);
        regionIndexStore.directCommit(false, (highwaters, scan) -> {
            scan.row(-1, new WALKey(regionName.toBytes()), new WALValue(null, orderIdProvider.nextId(), true));
        });

        RegionStore regionPropertiesStore = regionIndex.get(RegionProvider.REGION_PROPERTIES);
        regionPropertiesStore.directCommit(false, (highwaters, scan) -> {
            scan.row(-1, new WALKey(regionName.toBytes()), new WALValue(null, orderIdProvider.nextId(), true));
        });
    }
}
