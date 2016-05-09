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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.RingMembership;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.SystemStriper;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.PartitionIndex.PartitionPropertiesStream;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionCreator implements RowChanges, VersionedPartitionProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static final VersionedPartitionName NODE_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "NODE_INDEX".getBytes()),
        VersionedPartitionName.STATIC_VERSION);
    public static final VersionedPartitionName RING_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "RING_INDEX".getBytes()),
        VersionedPartitionName.STATIC_VERSION);
    public static final VersionedPartitionName REGION_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "REGION_INDEX".getBytes()),
        VersionedPartitionName.STATIC_VERSION);
    public static final VersionedPartitionName REGION_PROPERTIES = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "REGION_PROPERTIES".getBytes()),
        VersionedPartitionName.STATIC_VERSION);
    public static final VersionedPartitionName HIGHWATER_MARK_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "HIGHWATER_MARK_INDEX".getBytes()),
        VersionedPartitionName.STATIC_VERSION);
    public static final VersionedPartitionName PARTITION_VERSION_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "PARTITION_VERSION_INDEX".getBytes()),
        VersionedPartitionName.STATIC_VERSION);
    public static final VersionedPartitionName AQUARIUM_STATE_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "AQUARIUM_STATE_INDEX".getBytes()),
        VersionedPartitionName.STATIC_VERSION);
    public static final VersionedPartitionName AQUARIUM_LIVELINESS_INDEX = new VersionedPartitionName(
        new PartitionName(true, AmzaRingReader.SYSTEM_RING, "AQUARIUM_LIVELINESS_INDEX".getBytes()),
        VersionedPartitionName.STATIC_VERSION);

    private final OrderIdProvider orderIdProvider;
    private final PartitionPropertyMarshaller partitionPropertyMarshaller;
    private final PartitionIndex partitionIndex;
    private final SystemWALStorage systemWALStorage;
    private final WALUpdated walUpdated;
    private final RowChanges rowChanges;
    private final BAInterner interner;

    private final ConcurrentMap<PartitionName, PartitionProperties> partitionProperties = Maps.newConcurrentMap();
    private final AtomicLong partitionPropertiesVersion = new AtomicLong();

    private static final PartitionProperties REPLICATED_PROPERTIES = new PartitionProperties(Durability.fsync_never,
        0, 0, 0, 0, 0, 0, 0, 0,
        true,
        Consistency.none,
        true,
        true,
        false,
        RowType.primary,
        "memory_persistent",
        null,
        -1,
        -1);

    private static final PartitionProperties NON_REPLICATED_PROPERTIES = new PartitionProperties(Durability.fsync_never,
        0, 0, 0, 0, 0, 0, 0, 0,
        true,
        Consistency.none,
        true,
        false,
        false,
        RowType.primary,
        "memory_persistent",
        null,
        Integer.MAX_VALUE,
        -1);

    private static final PartitionProperties AQUARIUM_PROPERTIES = new PartitionProperties(Durability.ephemeral,
        0, 0, 0, 0, 0, 0, 0, 0,
        false,
        Consistency.none,
        true,
        true,
        false,
        RowType.primary,
        "memory_ephemeral",
        null,
        16,
        4);

    public static final Map<VersionedPartitionName, PartitionProperties> SYSTEM_PARTITIONS = ImmutableMap
        .<VersionedPartitionName, PartitionProperties>builder()
        .put(PartitionCreator.REGION_INDEX, REPLICATED_PROPERTIES)
        .put(PartitionCreator.RING_INDEX, REPLICATED_PROPERTIES)
        .put(PartitionCreator.NODE_INDEX, REPLICATED_PROPERTIES)
        .put(PartitionCreator.PARTITION_VERSION_INDEX, REPLICATED_PROPERTIES)
        .put(PartitionCreator.REGION_PROPERTIES, REPLICATED_PROPERTIES)
        .put(PartitionCreator.HIGHWATER_MARK_INDEX, NON_REPLICATED_PROPERTIES)
        .put(PartitionCreator.AQUARIUM_STATE_INDEX, AQUARIUM_PROPERTIES)
        .put(PartitionCreator.AQUARIUM_LIVELINESS_INDEX, AQUARIUM_PROPERTIES)
        .build();

    public PartitionCreator(OrderIdProvider orderIdProvider,
        PartitionPropertyMarshaller partitionPropertyMarshaller,
        PartitionIndex partitionIndex,
        SystemWALStorage systemWALStorage,
        WALUpdated walUpdated,
        RowChanges rowChanges,
        BAInterner interner) {

        this.orderIdProvider = orderIdProvider;
        this.partitionPropertyMarshaller = partitionPropertyMarshaller;
        this.partitionIndex = partitionIndex;
        this.walUpdated = walUpdated;
        this.systemWALStorage = systemWALStorage;
        this.rowChanges = rowChanges;
        this.interner = interner;
    }

    public void init(SystemStriper systemStriper) throws Exception {
        for (Map.Entry<VersionedPartitionName, PartitionProperties> entry : SYSTEM_PARTITIONS.entrySet()) {
            VersionedPartitionName versionedPartitionName = entry.getKey();
            int systemStripe = systemStriper.getSystemStripe(versionedPartitionName.getPartitionName());
            partitionIndex.get(versionedPartitionName, entry.getValue(), systemStripe);
        }
    }

    @Override
    public boolean hasPartition(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return true;
        }

        byte[] rawPartitionName = partitionName.toBytes();
        TimestampedValue propertiesValue = systemWALStorage.getTimestampedValue(REGION_PROPERTIES, null, rawPartitionName);
        if (propertiesValue != null) {
            TimestampedValue indexValue = systemWALStorage.getTimestampedValue(REGION_INDEX, null, rawPartitionName);
            if (indexValue != null) {
                return true;
            }
        }
        return false;
    }

    public boolean hasStore(VersionedPartitionName versionedPartitionName, int stripeIndex) throws Exception {
        PartitionProperties properties = getProperties(versionedPartitionName.getPartitionName());
        return properties != null && partitionIndex.exists(versionedPartitionName, properties, stripeIndex);
    }

    public PartitionStore createStoreIfAbsent(VersionedPartitionName versionedPartitionName, int stripe) throws Exception {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        Preconditions.checkArgument(!partitionName.isSystemPartition(), "You cannot create a system partition");

        PartitionProperties properties = getProperties(partitionName);
        if (properties == null) {
            return null;
        } else {
            return partitionIndex.get(versionedPartitionName, properties, stripe);
        }
    }

    public boolean createPartitionIfAbsent(PartitionName partitionName, PartitionProperties properties) throws Exception {
        byte[] rawPartitionName = partitionName.toBytes();
        TimestampedValue regionIndexValue = systemWALStorage.getTimestampedValue(REGION_INDEX, null, rawPartitionName);
        long timestampAndVersion;
        if (regionIndexValue != null) {
            timestampAndVersion = regionIndexValue.getTimestampId();
        } else {
            timestampAndVersion = orderIdProvider.nextId();
            RowsChanged changed = systemWALStorage.update(REGION_INDEX, null,
                (highwater, scan) -> scan.row(-1, rawPartitionName, rawPartitionName, timestampAndVersion, false, timestampAndVersion),
                walUpdated);
            if (!changed.isEmpty()) {
                rowChanges.changes(changed);
            }
        }

        TimestampedValue propertiesValue = systemWALStorage.getTimestampedValue(REGION_PROPERTIES, null, rawPartitionName);
        if (propertiesValue == null) {
            return setPartitionProperties(partitionName, properties, timestampAndVersion);
        } else {
            return false;
        }
    }

    public void updatePartitionProperties(PartitionName partitionName, PartitionProperties properties) throws Exception {
        byte[] rawPartitionName = partitionName.toBytes();
        TimestampedValue regionIndexValue = systemWALStorage.getTimestampedValue(REGION_INDEX, null, rawPartitionName);
        if (regionIndexValue == null) {
            throw new IllegalArgumentException("Partition has not been initialized: " + partitionName);
        }

        long timestampAndVersion = orderIdProvider.nextId();
        setPartitionProperties(partitionName, properties, timestampAndVersion);
    }

    private boolean setPartitionProperties(PartitionName partitionName, PartitionProperties properties, long timestampAndVersion) throws Exception {
        byte[] partitionNameBytes = partitionName.toBytes();

        RowsChanged changed = systemWALStorage.update(REGION_PROPERTIES, null, (highwater, scan) -> {
            return scan.row(-1, partitionNameBytes, partitionPropertyMarshaller.toBytes(properties), timestampAndVersion, false, timestampAndVersion);
        }, walUpdated);

        if (!changed.isEmpty()) {
            rowChanges.changes(changed);
            partitionProperties.put(partitionName, properties);
            return true;
        }
        return false;
    }

    private void removeProperties(PartitionName partitionName) {
        partitionProperties.remove(partitionName);
    }

    @Override
    public PartitionProperties getProperties(PartitionName partitionName) {

        return partitionProperties.computeIfAbsent(partitionName, (key) -> {
            try {
                if (partitionName.isSystemPartition()) {
                    return SYSTEM_PARTITIONS.get(new VersionedPartitionName(partitionName, VersionedPartitionName.STATIC_VERSION));
                } else {
                    TimestampedValue rawPartitionProperties = systemWALStorage.getTimestampedValue(REGION_PROPERTIES, null, partitionName.toBytes());
                    if (rawPartitionProperties == null) {
                        return null;
                    }
                    return partitionPropertyMarshaller.fromBytes(rawPartitionProperties.getValue());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public VersionedPartitionProperties getVersionedProperties(PartitionName partitionName, VersionedPartitionProperties versionedPartitionProperties) {
        long version = partitionPropertiesVersion.get();
        if (versionedPartitionProperties != null && versionedPartitionProperties.version >= version) {
            return versionedPartitionProperties;
        }
        return new VersionedPartitionProperties(version, getProperties(partitionName));
    }

    public PartitionStore get(VersionedPartitionName versionedPartitionName, int stripeIndex) throws Exception {
        PartitionProperties properties = getProperties(versionedPartitionName.getPartitionName());
        return partitionIndex.get(versionedPartitionName, properties, stripeIndex);
    }

    @Override
    public Iterable<PartitionName> getMemberPartitions(RingMembership ringMembership) throws Exception {
        List<PartitionName> partitionNames = Lists.newArrayList();
        systemWALStorage.rowScan(REGION_INDEX, (rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
            if (!valueTombstoned && valueTimestamp != -1 && value != null) {
                PartitionName partitionName = PartitionName.fromBytes(key, 0, interner);
                if (ringMembership == null || ringMembership.isMemberOfRing(partitionName.getRingName())) {
                    partitionNames.add(partitionName);
                }
            }
            return true;
        });
        return partitionNames;
    }

    public void streamAllParitions(PartitionPropertiesStream partitionStream) throws Exception {
        systemWALStorage.rowScan(REGION_PROPERTIES, (rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
            if (!valueTombstoned) {
                PartitionName partitionName = PartitionName.fromBytes(key, 0, interner);
                PartitionProperties properties = partitionPropertyMarshaller.fromBytes(value);
                if (!partitionStream.stream(partitionName, properties)) {
                    return false;
                }
            }
            return true;
        });
    }

    public void markForDisposal(PartitionName partitionName) throws Exception {
        byte[] rawPartitionName = partitionName.toBytes();
        long timestampAndVersion = orderIdProvider.nextId();
        RowsChanged changed = systemWALStorage.update(REGION_INDEX, null,
            (highwater, scan) -> scan.row(-1, rawPartitionName, null, timestampAndVersion, false, timestampAndVersion),
            walUpdated);
        if (!changed.isEmpty()) {
            rowChanges.changes(changed);
        }
    }

    @Override
    public boolean isPartitionDisposed(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return false;
        }

        byte[] rawPartitionName = partitionName.toBytes();
        TimestampedValue indexValue = systemWALStorage.getTimestampedValue(REGION_INDEX, null, rawPartitionName);
        return (indexValue != null && indexValue.getValue() == null);
    }

    public void destroyPartition(PartitionName partitionName) throws Exception {
        Preconditions.checkArgument(!partitionName.isSystemPartition(), "You cannot destroy a system partition");
        long timestampAndVersion = orderIdProvider.nextId();

        systemWALStorage.update(REGION_PROPERTIES, null, (highwaters, scan) -> {
            return scan.row(-1, partitionName.toBytes(), null, timestampAndVersion, true, timestampAndVersion);
        }, walUpdated);

        systemWALStorage.update(REGION_INDEX, null, (highwaters, scan) -> {
            return scan.row(-1, partitionName.toBytes(), null, timestampAndVersion, true, timestampAndVersion);
        }, walUpdated);

        partitionProperties.remove(partitionName);
        partitionIndex.invalidate(partitionName);
    }

    public Iterable<VersionedPartitionName> getSystemPartitions() {
        return SYSTEM_PARTITIONS.keySet();
    }

    @Override
    public void changes(final RowsChanged changes) throws Exception {
        if (changes.getVersionedPartitionName().getPartitionName().equals(REGION_PROPERTIES.getPartitionName())) {
            try {
                for (Map.Entry<WALKey, WALValue> entry : changes.getApply().entrySet()) {
                    PartitionName partitionName = PartitionName.fromBytes(entry.getKey().key, 0, interner);
                    removeProperties(partitionName);

                    PartitionProperties properties = getProperties(partitionName);
                    partitionIndex.updateStoreProperties(partitionName, properties);

                }
                partitionPropertiesVersion.incrementAndGet();
            } catch (Throwable ex) {
                throw new RuntimeException("Error while streaming entry set.", ex);
            }
        }
    }
}
