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

import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeReplicator;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

public class RegionProvider {

    public static final RegionName RING_INDEX = new RegionName(true, "system", "RING_INDEX");
    public static final RegionName REGION_INDEX = new RegionName(true, "system", "REGION_INDEX");
    public static final RegionName REGION_PROPERTIES = new RegionName(true, "system", "REGION_PROPERTIES");
    public static final RegionName HIGHWATER_MARK_INDEX = new RegionName(true, "system", "HIGHWATER_MARKS");

    private final OrderIdProvider orderIdProvider;
    private final RegionPropertyMarshaller regionPropertyMarshaller;
    private final AmzaRegionChangeReplicator replicator;
    private final RegionIndex regionIndex;
    private final RowChanges rowChanges;
    private final boolean hardFlush;

    public RegionProvider(OrderIdProvider orderIdProvider,
        RegionPropertyMarshaller regionPropertyMarshaller,
        AmzaRegionChangeReplicator replicator,
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

    public RegionStore createRegionStoreIfAbsent(RegionName regionName, RegionProperties properties) throws Exception {
        if (regionName.isSystemRegion()) {
            throw new IllegalArgumentException("You cannot create system regions.");
        }
        RegionStore regionStore = regionIndex.get(regionName);
        if (regionStore == null) {
            setRegionProperties(regionName, properties);
            regionStore = regionIndex.get(regionName);
            if (regionStore != null) {
                final byte[] rawRegionName = regionName.toBytes();
                final WALKey regionKey = new WALKey(rawRegionName);
                RegionStore regionIndexStore = regionName.equals(REGION_INDEX) ? regionStore : regionIndex.get(REGION_INDEX);
                RowsChanged changed = regionIndexStore.directCommit(false, replicator, WALStorageUpdateMode.replicateThenUpdate, new Scannable<WALValue>() {

                    @Override
                    public void rowScan(Scan<WALValue> scan) throws Exception {
                        scan.row(-1, regionKey, new WALValue(rawRegionName, orderIdProvider.nextId(), false));
                    }
                });
                regionIndexStore.flush(hardFlush);
                if (!changed.isEmpty()) {
                    rowChanges.changes(changed);
                }
            }
        }
        return regionStore;
    }

    public void setRegionProperties(final RegionName regionName, final RegionProperties properties) throws Exception {
        regionIndex.putProperties(regionName, properties);
        RegionStore regionPropertiesStore = regionIndex.get(REGION_PROPERTIES);
        RowsChanged changed = regionPropertiesStore.directCommit(false, replicator, WALStorageUpdateMode.replicateThenUpdate, new Scannable<WALValue>() {

            @Override
            public void rowScan(Scan<WALValue> scan) throws Exception {
                scan.row(-1, new WALKey(regionName.toBytes()), new WALValue(regionPropertyMarshaller.toBytes(properties), orderIdProvider.nextId(), false));
            }
        });
        regionPropertiesStore.flush(hardFlush);
        if (!changed.isEmpty()) {
            rowChanges.changes(changed);
        }
    }

}
