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
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

public class PartitionProvider {

    public static final VersionedPartitionName NODE_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "NODE_INDEX"), 0);
    public static final VersionedPartitionName RING_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "RING_INDEX"), 0);
    public static final VersionedPartitionName REGION_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "REGION_INDEX"), 0);
    public static final VersionedPartitionName REGION_PROPERTIES = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "REGION_PROPERTIES"), 0);
    public static final VersionedPartitionName HIGHWATER_MARK_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "HIGHWATER_MARK_INDEX"), 0);
    public static final VersionedPartitionName REGION_ONLINE_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "REGION_ONLINE_INDEX"), 0);

    private final OrderIdProvider orderIdProvider;
    private final PartitionPropertyMarshaller partitionPropertyMarshaller;
    private final PartitionIndex partitionIndex;
    private final SystemWALStorage systemWALStorage;
    private final WALUpdated walUpdated;
    private final RowChanges rowChanges;

    public PartitionProvider(OrderIdProvider orderIdProvider,
        PartitionPropertyMarshaller partitionPropertyMarshaller,
        PartitionIndex partitionIndex,
        SystemWALStorage systemWALStorage,
        WALUpdated walUpdated,
        RowChanges rowChanges) {

        this.orderIdProvider = orderIdProvider;
        this.partitionPropertyMarshaller = partitionPropertyMarshaller;
        this.partitionIndex = partitionIndex;
        this.walUpdated = walUpdated;
        this.systemWALStorage = systemWALStorage;
        this.rowChanges = rowChanges;
    }

    public boolean hasPartition(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return true;
        }

        byte[] rawPartitionName = partitionName.toBytes();
        WALKey partitionKey = new WALKey(rawPartitionName);
        WALValue propertiesValue = systemWALStorage.get(REGION_PROPERTIES, partitionKey);
        if (propertiesValue != null && !propertiesValue.getTombstoned()) {
            WALValue indexValue = systemWALStorage.get(REGION_INDEX, partitionKey);
            if (indexValue != null && !indexValue.getTombstoned()) {
                return true;
            }
        }
        return false;
    }

    public boolean createPartitionStoreIfAbsent(VersionedPartitionName versionedPartitionName,
        PartitionProperties properties) throws Exception {

        PartitionName partitionName = versionedPartitionName.getPartitionName();
        Preconditions.checkArgument(!partitionName.isSystemPartition(), "You cannot create a system partition");

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        if (partitionStore == null) {
            updatePartitionProperties(partitionName, properties);

            byte[] rawPartitionName = partitionName.toBytes();
            WALKey partitionKey = new WALKey(rawPartitionName);
            RowsChanged changed = systemWALStorage.update(REGION_INDEX, (highwater, scan) -> {
                scan.row(-1, partitionKey, new WALValue(rawPartitionName, orderIdProvider.nextId(), false));
            }, walUpdated);
            if (!changed.isEmpty()) {
                rowChanges.changes(changed);
            }
            partitionIndex.get(versionedPartitionName);
            return true;
        } else {
            return false;
        }
    }

    public void updatePartitionProperties(PartitionName partitionName, PartitionProperties properties) throws Exception {
        RowsChanged changed = systemWALStorage.update(REGION_PROPERTIES, (highwater, scan) -> {
            scan.row(-1, new WALKey(partitionName.toBytes()), new WALValue(partitionPropertyMarshaller.toBytes(properties), orderIdProvider.nextId(), false));
        }, walUpdated);
        if (!changed.isEmpty()) {
            rowChanges.changes(changed);
        }
        partitionIndex.putProperties(partitionName, properties);
    }

    public void destroyPartition(PartitionName partitionName) throws Exception {
        Preconditions.checkArgument(!partitionName.isSystemPartition(), "You cannot destroy a system partition");

        systemWALStorage.update(REGION_INDEX, (highwaters, scan) -> {
            scan.row(-1, new WALKey(partitionName.toBytes()), new WALValue(null, orderIdProvider.nextId(), true));
        }, walUpdated);

        systemWALStorage.update(REGION_PROPERTIES, (highwaters, scan) -> {
            scan.row(-1, new WALKey(partitionName.toBytes()), new WALValue(null, orderIdProvider.nextId(), true));
        }, walUpdated);
    }
}
