package com.jivesoftware.os.amza.service.storage;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.api.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.Commitable;
import com.jivesoftware.os.amza.api.scan.RowType;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stream.KeyContainedStream;
import com.jivesoftware.os.amza.shared.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;

/**
 * @author jonathan.colt
 */
public class SystemWALStorage {

    private static final Predicate<VersionedPartitionName> IS_SYSTEM_PREDICATE = input -> input.getPartitionName().isSystemPartition();

    private final PartitionIndex partitionIndex;
    private final PrimaryRowMarshaller<byte[]> rowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final RowChanges allRowChanges;
    private final boolean hardFlush;

    public SystemWALStorage(PartitionIndex partitionIndex,
        PrimaryRowMarshaller<byte[]> rowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        RowChanges allRowChanges,
        boolean hardFlush) {
        this.partitionIndex = partitionIndex;
        this.rowMarshaller = rowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.allRowChanges = allRowChanges;
        this.hardFlush = hardFlush;
    }

    public RowsChanged update(VersionedPartitionName versionedPartitionName,
        byte[] prefix,
        Commitable updates,
        WALUpdated updated) throws Exception {

        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        RowsChanged changed = partitionStore.getWalStorage().update(-1, false, prefix, updates);
        if (allRowChanges != null && !changed.isEmpty()) {
            allRowChanges.changes(changed);
        }
        if (!changed.getApply().isEmpty()) {
            //LOG.info("UPDATED:{} txId:{}", versionedPartitionName, changed.getLargestCommittedTxId());
            updated.updated(versionedPartitionName, Status.ONLINE, changed.getLargestCommittedTxId());
        }
        partitionStore.flush(hardFlush);
        return changed;
    }

    public TimestampedValue getTimestampedValue(VersionedPartitionName versionedPartitionName, byte[] prefix, byte[] key) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).getTimestampedValue(prefix, key);
    }

    public boolean get(VersionedPartitionName versionedPartitionName,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueStream stream) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).streamValues(prefix, keys, (_prefix, key, value, valueTimestamp, valueTombstone) -> {
            if (valueTimestamp == -1) {
                return stream.stream(prefix, key, null, -1, false);
            } else {
                return stream.stream(prefix, key, value, valueTimestamp, valueTombstone);
            }
        });
    }

    public boolean containsKeys(VersionedPartitionName versionedPartitionName,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyContainedStream stream) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).containsKeys(prefix, keys, stream);
    }

    public <R> R takeRowUpdatesSince(VersionedPartitionName versionedPartitionName,
        long transactionId,
        PartitionStripe.TakeRowUpdates<R> takeRowUpdates) throws Exception {

        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        PartitionStripe.RowStreamer streamer = rowStream -> partitionStore.takeRowUpdatesSince(transactionId, rowStream);
        return takeRowUpdates.give(versionedPartitionName, TxPartitionStatus.Status.ONLINE, streamer);
    }

    public boolean takeFromTransactionId(VersionedPartitionName versionedPartitionName,
        long transactionId,
        Highwaters highwaters,
        TxKeyValueStream txKeyValueStream)
        throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).getWalStorage().takeRowUpdatesSince(transactionId,
            (rowFP, rowTxId, rowType, row) -> {
                if (rowType == RowType.highwater && highwaters != null) {
                    WALHighwater highwater = highwaterRowMarshaller.fromBytes(row);
                    highwaters.highwater(highwater);
                } else if (rowType == RowType.primary && rowTxId > transactionId) {
                    return rowMarshaller.fromRows(txFpRowStream -> txFpRowStream.stream(rowTxId, rowFP, row), txKeyValueStream);
                }
                return true;
            });
    }

    public boolean takeFromTransactionId(VersionedPartitionName versionedPartitionName,
        byte[] prefix,
        long transactionId,
        Highwaters highwaters,
        TxKeyValueStream txKeyValueStream)
        throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).getWalStorage().takeRowUpdatesSince(prefix, transactionId,
            (rowFP, rowTxId, rowType, row) -> {
                if (rowType == RowType.highwater && highwaters != null) {
                    WALHighwater highwater = highwaterRowMarshaller.fromBytes(row);
                    highwaters.highwater(highwater);
                } else if (rowType == RowType.primary && rowTxId > transactionId) {
                    return rowMarshaller.fromRows(txFpRowStream -> txFpRowStream.stream(rowTxId, rowFP, row), txKeyValueStream);
                }
                return true;
            });
    }

    public boolean takeRowsFromTransactionId(VersionedPartitionName versionedPartitionName, long transactionId, RowStream rowStream)
        throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).getWalStorage().takeRowUpdatesSince(transactionId, rowStream);
    }

    public boolean rowScan(VersionedPartitionName versionedPartitionName, KeyValueStream keyValueStream) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            return partitionIndex.get(versionedPartitionName).getWalStorage().rowScan(keyValueStream);
        }
    }

    public boolean rangeScan(VersionedPartitionName versionedPartitionName,
        byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        KeyValueStream keyValueStream) throws Exception {

        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            return partitionIndex.get(versionedPartitionName).getWalStorage().rangeScan(fromPrefix, fromKey, toPrefix, toKey, keyValueStream);
        }
    }

    public void highestPartitionTxIds(HighestPartitionTx tx) throws Exception {
        for (VersionedPartitionName versionedPartitionName : Iterables.filter(partitionIndex.getAllPartitions(), IS_SYSTEM_PREDICATE)) {
            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore != null) {
                long highestTxId = partitionStore.getWalStorage().highestTxId();
                tx.tx(versionedPartitionName, Status.ONLINE, highestTxId);
            }
        }
    }

    public long count(VersionedPartitionName versionedPartitionName) throws Exception {
        return partitionIndex.get(versionedPartitionName).getWalStorage().count(keyStream -> true);
    }
}
