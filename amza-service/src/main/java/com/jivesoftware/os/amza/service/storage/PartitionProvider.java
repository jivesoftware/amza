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
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

public class PartitionProvider {

    public static final VersionedPartitionName NODE_INDEX = new VersionedPartitionName(new PartitionName(true, "system", "NODE_INDEX"), 0);
    public static final VersionedPartitionName RING_INDEX = new VersionedPartitionName(new PartitionName(true, "system", "RING_INDEX"), 0);
    public static final VersionedPartitionName REGION_INDEX = new VersionedPartitionName(new PartitionName(true, "system", "REGION_INDEX"), 0);
    public static final VersionedPartitionName REGION_PROPERTIES = new VersionedPartitionName(new PartitionName(true, "system", "REGION_PROPERTIES"), 0);
    public static final VersionedPartitionName HIGHWATER_MARK_INDEX = new VersionedPartitionName(new PartitionName(true, "system", "HIGHWATER_MARK_INDEX"), 0);
    public static final VersionedPartitionName REGION_ONLINE_INDEX = new VersionedPartitionName(new PartitionName(true, "system", "REGION_ONLINE_INDEX"), 0);

    private final OrderIdProvider orderIdProvider;
    private final PartitionPropertyMarshaller partitionPropertyMarshaller;
    private final PartitionIndex partitionIndex;
    private final RowChanges rowChanges;
    private final boolean hardFlush;

    public PartitionProvider(OrderIdProvider orderIdProvider,
        PartitionPropertyMarshaller partitionPropertyMarshaller,
        PartitionIndex partitionIndex,
        RowChanges rowChanges,
        boolean hardFlush) {

        this.orderIdProvider = orderIdProvider;
        this.partitionPropertyMarshaller = partitionPropertyMarshaller;
        this.partitionIndex = partitionIndex;
        this.rowChanges = rowChanges;
        this.hardFlush = hardFlush;
    }

    public boolean hasPartition(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return true;
        }

        final byte[] rawPartitionName = partitionName.toBytes();
        final WALKey partitionKey = new WALKey(rawPartitionName);
        PartitionStore partitionPropertiesStore = partitionIndex.get(REGION_PROPERTIES);
        WALValue propertiesValue = partitionPropertiesStore.get(partitionKey);
        if (propertiesValue != null && !propertiesValue.getTombstoned()) {
            PartitionStore partitionIndexStore = partitionIndex.get(REGION_INDEX);
            WALValue indexValue = partitionIndexStore.get(partitionKey);
            if (indexValue != null && !indexValue.getTombstoned()) {
                return true;
            }
        }
        return false;
    }

    public void createPartitionStoreIfAbsent(VersionedPartitionName versionedPartitionName, PartitionProperties properties) throws Exception {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        Preconditions.checkArgument(!partitionName.isSystemPartition(), "You cannot create a system partition");

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        if (partitionStore == null) {
            updatePartitionProperties(partitionName, properties);

            byte[] rawPartitionName = partitionName.toBytes();
            WALKey partitionKey = new WALKey(rawPartitionName);
            PartitionStore partitionIndexStore = partitionIndex.get(REGION_INDEX);
            RowsChanged changed = partitionIndexStore.directCommit(false, (highwater, scan) -> {
                scan.row(-1, partitionKey, new WALValue(rawPartitionName, orderIdProvider.nextId(), false));
            });
            partitionIndexStore.flush(hardFlush);
            if (!changed.isEmpty()) {
                rowChanges.changes(changed);
            }
            partitionIndex.get(versionedPartitionName);
        }
    }

    public void updatePartitionProperties(PartitionName partitionName, PartitionProperties properties) throws Exception {
        PartitionStore partitionPropertiesStore = partitionIndex.get(REGION_PROPERTIES);
        RowsChanged changed = partitionPropertiesStore.directCommit(false, (highwater, scan) -> {
            scan.row(-1, new WALKey(partitionName.toBytes()), new WALValue(partitionPropertyMarshaller.toBytes(properties), orderIdProvider.nextId(), false));
        });
        partitionPropertiesStore.flush(hardFlush);
        if (!changed.isEmpty()) {
            rowChanges.changes(changed);
        }
        partitionIndex.putProperties(partitionName, properties);
    }

    public void destroyPartition(PartitionName partitionName) throws Exception {
        Preconditions.checkArgument(!partitionName.isSystemPartition(), "You cannot destroy a system partition");

        PartitionStore partitionIndexStore = partitionIndex.get(PartitionProvider.REGION_INDEX);
        partitionIndexStore.directCommit(false, (highwaters, scan) -> {
            scan.row(-1, new WALKey(partitionName.toBytes()), new WALValue(null, orderIdProvider.nextId(), true));
        });

        PartitionStore partitionPropertiesStore = partitionIndex.get(PartitionProvider.REGION_PROPERTIES);
        partitionPropertiesStore.directCommit(false, (highwaters, scan) -> {
            scan.row(-1, new WALKey(partitionName.toBytes()), new WALValue(null, orderIdProvider.nextId(), true));
        });
    }
}
