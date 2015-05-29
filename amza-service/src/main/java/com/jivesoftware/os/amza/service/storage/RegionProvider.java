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

import com.jivesoftware.os.amza.service.replication.RegionChangeReplicator;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
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
    private final RegionChangeReplicator replicator;
    private final RegionIndex regionIndex;
    private final RowChanges rowChanges;
    private final boolean hardFlush;

    public RegionProvider(OrderIdProvider orderIdProvider,
        RegionPropertyMarshaller regionPropertyMarshaller,
        RegionChangeReplicator replicator,
        RegionIndex regionIndex,
        RowChanges rowChanges,
        boolean hardFlush) {

        this.orderIdProvider = orderIdProvider;
        this.regionPropertyMarshaller = regionPropertyMarshaller;
        this.replicator = replicator;
        this.regionIndex = regionIndex;
        this.rowChanges = rowChanges;
        this.hardFlush = hardFlush;
    }

    public void createRegionStoreIfAbsent(VersionedRegionName versionedRegionName, RegionProperties properties) throws Exception {
        if (versionedRegionName.getRegionName().isSystemRegion()) {
            throw new IllegalArgumentException("You cannot create system regions.");
        }
        RegionStore regionStore = regionIndex.get(versionedRegionName);
        if (regionStore == null) {
            updateRegionProperties(versionedRegionName.getRegionName(), properties);
            regionStore = regionIndex.get(versionedRegionName);
            if (regionStore != null) {
                final byte[] rawRegionName = versionedRegionName.toBytes();
                final WALKey regionKey = new WALKey(rawRegionName);
                RegionStore regionIndexStore = versionedRegionName.equals(REGION_INDEX) ? regionStore : regionIndex.get(REGION_INDEX);
                RowsChanged changed = regionIndexStore.directCommit(false, replicator, WALStorageUpdateMode.replicateThenUpdate, (highwater, scan) -> {
                    scan.row(-1, regionKey, new WALValue(rawRegionName, orderIdProvider.nextId(), false));
                });
                regionIndexStore.flush(hardFlush);
                if (!changed.isEmpty()) {
                    rowChanges.changes(changed);
                }
            }
        }
    }

    public void updateRegionProperties(final RegionName regionName, final RegionProperties properties) throws Exception {
        regionIndex.putProperties(regionName, properties);
        RegionStore regionPropertiesStore = regionIndex.get(REGION_PROPERTIES);
        RowsChanged changed = regionPropertiesStore.directCommit(false, replicator, WALStorageUpdateMode.replicateThenUpdate, (highwater, scan) -> {
            scan.row(-1, new WALKey(regionName.toBytes()), new WALValue(regionPropertyMarshaller.toBytes(properties), orderIdProvider.nextId(), false));
        });
        regionPropertiesStore.flush(hardFlush);
        if (!changed.isEmpty()) {
            rowChanges.changes(changed);
        }
    }

}
