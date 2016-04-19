package com.jivesoftware.os.amza.service.storage;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.aquarium.LivelyEndState;

/**
 * @author jonathan.colt
 */
public class SystemWALStorage {

    private final PartitionIndex partitionIndex;
    private final PrimaryRowMarshaller rowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final RowChanges allRowChanges;
    private final boolean hardFlush;

    public SystemWALStorage(PartitionIndex partitionIndex,
        PrimaryRowMarshaller rowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        RowChanges allRowChanges,
        boolean hardFlush) {
        this.partitionIndex = partitionIndex;
        this.rowMarshaller = rowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.allRowChanges = allRowChanges;
        this.hardFlush = hardFlush;
    }

    public void load(HighestPartitionTx tx) throws Exception {
        highestPartitionTxIds(tx);
    }

    public RowsChanged update(VersionedPartitionName versionedPartitionName,
        byte[] prefix,
        Commitable updates,
        WALUpdated updated) throws Exception {

        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        PartitionStore partitionStore = partitionIndex.getSystemPartition(versionedPartitionName);
        RowsChanged changed = partitionStore.getWalStorage().update(true, partitionStore.getProperties().rowType, -1, false, prefix, updates);
        if (allRowChanges != null && !changed.isEmpty()) {
            allRowChanges.changes(changed);
        }
        if (!changed.getApply().isEmpty()) {
            //LOG.info("UPDATED:{} txId:{}", versionedPartitionName, changed.getLargestCommittedTxId());
            updated.updated(versionedPartitionName, changed.getLargestCommittedTxId());
        }
        partitionStore.flush(hardFlush);
        return changed;
    }

    public TimestampedValue getTimestampedValue(VersionedPartitionName versionedPartitionName, byte[] prefix, byte[] key) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.getSystemPartition(versionedPartitionName).getTimestampedValue(prefix, key);
    }

    public boolean get(VersionedPartitionName versionedPartitionName,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueStream stream) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.getSystemPartition(versionedPartitionName).streamValues(prefix, keys,
            (rowType, _prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                if (valueTimestamp == -1) {
                    return stream.stream(rowType, prefix, key, null, -1, false, -1);
                } else {
                    return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion);
                }
            });
    }

    public boolean containsKeys(VersionedPartitionName versionedPartitionName,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyContainedStream stream) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.getSystemPartition(versionedPartitionName).containsKeys(prefix, keys, stream);
    }

    public <R> R takeRowUpdatesSince(VersionedPartitionName versionedPartitionName,
        long transactionId,
        PartitionStripe.TakeRowUpdates<R> takeRowUpdates) throws Exception {

        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.getSystemPartition(versionedPartitionName);
        PartitionStripe.RowStreamer streamer = rowStream -> partitionStore.takeRowUpdatesSince(transactionId, rowStream);
        return takeRowUpdates.give(versionedPartitionName, LivelyEndState.ALWAYS_ONLINE, streamer);
    }

    public boolean takeFromTransactionId(VersionedPartitionName versionedPartitionName,
        long transactionId,
        Highwaters highwaters,
        TxKeyValueStream txKeyValueStream)
        throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.getSystemPartition(versionedPartitionName).getWalStorage().takeRowUpdatesSince(transactionId,
            (rowFP, rowTxId, rowType, row) -> {
                if (rowType == RowType.highwater && highwaters != null) {
                    WALHighwater highwater = highwaterRowMarshaller.fromBytes(row);
                    highwaters.highwater(highwater);
                } else if (rowType.isPrimary() && rowTxId > transactionId) {
                    return rowMarshaller.fromRows(txFpRowStream -> txFpRowStream.stream(rowTxId, rowFP, rowType, row), txKeyValueStream);
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
        return partitionIndex.getSystemPartition(versionedPartitionName).getWalStorage().takeRowUpdatesSince(prefix, transactionId,
            (rowFP, rowTxId, rowType, row) -> {
                if (rowType == RowType.highwater && highwaters != null) {
                    WALHighwater highwater = highwaterRowMarshaller.fromBytes(row);
                    highwaters.highwater(highwater);
                } else if (rowType.isPrimary() && rowTxId > transactionId) {
                    return rowMarshaller.fromRows(txFpRowStream -> txFpRowStream.stream(rowTxId, rowFP, rowType, row), txKeyValueStream);
                }
                return true;
            });
    }

    public boolean takeRowsFromTransactionId(VersionedPartitionName versionedPartitionName, long transactionId, RowStream rowStream)
        throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.getSystemPartition(versionedPartitionName).getWalStorage().takeRowUpdatesSince(transactionId, rowStream);
    }

    public boolean rowScan(VersionedPartitionName versionedPartitionName, KeyValueStream keyValueStream) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.getSystemPartition(versionedPartitionName);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            return partitionStore.getWalStorage().rowScan(keyValueStream);
        }
    }

    public boolean rangeScan(VersionedPartitionName versionedPartitionName,
        byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        KeyValueStream keyValueStream) throws Exception {

        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.getSystemPartition(versionedPartitionName);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            return partitionStore.getWalStorage().rangeScan(fromPrefix, fromKey, toPrefix, toKey, keyValueStream);
        }
    }

    private void highestPartitionTxIds(HighestPartitionTx tx) throws Exception {
        for (VersionedPartitionName versionedPartitionName : partitionIndex.getSystemPartitions()) {
            PartitionStore partitionStore = partitionIndex.getSystemPartition(versionedPartitionName);
            if (partitionStore != null) {
                long highestTxId = partitionStore.getWalStorage().highestTxId();
                tx.tx(new VersionedAquarium(versionedPartitionName, null, 0), highestTxId);
            }
        }
    }

    public long highestPartitionTxId(VersionedPartitionName versionedPartitionName, HighestPartitionTx tx) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        PartitionStore partitionStore = partitionIndex.getSystemPartition(versionedPartitionName);
        if (partitionStore != null) {
            long highestTxId = partitionStore.getWalStorage().highestTxId();
            return tx.tx(new VersionedAquarium(versionedPartitionName, null, 0), highestTxId);
        } else {
            return tx.tx(null, -1);
        }

    }

    public long count(VersionedPartitionName versionedPartitionName) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.getSystemPartition(versionedPartitionName).getWalStorage().count(keyStream -> true);
    }
}
