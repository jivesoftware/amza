package com.jivesoftware.os.amza.service.storage;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.TxKeyValueStream;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.TimestampKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;

/**
 * @author jonathan.colt
 */
public class SystemWALStorage {

    private static final Predicate<VersionedPartitionName> IS_SYSTEM_PREDICATE = input -> input.getPartitionName().isSystemPartition();

    private final PartitionIndex partitionIndex;
    private final RowChanges allRowChanges;
    private final boolean hardFlush;

    public SystemWALStorage(PartitionIndex partitionIndex, RowChanges allRowChanges, boolean hardFlush) {
        this.partitionIndex = partitionIndex;
        this.allRowChanges = allRowChanges;
        this.hardFlush = hardFlush;
    }

    public RowsChanged update(VersionedPartitionName versionedPartitionName,
        Commitable updates,
        WALUpdated updated) throws Exception {

        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        RowsChanged changed = partitionStore.getWalStorage().update(false, updates);
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

    public TimestampedValue get(VersionedPartitionName versionedPartitionName, byte[] key) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).get(key);
    }

    public boolean get(VersionedPartitionName versionedPartitionName, WALKeys keys, TimestampKeyValueStream stream) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).get(keys, (key, value, valueTimestamp, valueTombstone) -> {
            if (value == null || valueTombstone) {
                return stream.stream(key, null, -1);
            } else {
                return stream.stream(key, value, valueTimestamp);
            }
        });
    }

    public boolean containsKeys(VersionedPartitionName versionedPartitionName, WALKeys keys, KeyContainedStream stream) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).containsKeys(keys, stream);
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
        return partitionIndex.get(versionedPartitionName).getWalStorage().takeFromTransactionId(transactionId, highwaters, txKeyValueStream);
    }

    public boolean takeRowsFromTransactionId(VersionedPartitionName versionedPartitionName, long transactionId, RowStream rowStream)
        throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).getWalStorage().takeRowsFromTransactionId(transactionId, rowStream);
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
        byte[] from,
        byte[] to,
        KeyValueStream keyValueStream) throws Exception {

        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            return partitionIndex.get(versionedPartitionName).getWalStorage().rangeScan(from, to, keyValueStream);
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
        return partitionIndex.get(versionedPartitionName).getWalStorage().count();
    }
}
