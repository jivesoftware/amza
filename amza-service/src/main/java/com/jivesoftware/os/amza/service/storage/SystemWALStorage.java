package com.jivesoftware.os.amza.service.storage;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorage;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;

/**
 * @author jonathan.colt
 */
public class SystemWALStorage {

    private final PartitionIndex partitionIndex;

    public SystemWALStorage(PartitionIndex partitionIndex) {
        this.partitionIndex = partitionIndex;
    }

    public RowsChanged update(VersionedPartitionName versionedPartitionName,
        Commitable<WALValue> updates, WALUpdated updated) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).getWalStorage().update(false, Status.ONLINE, updates, updated);
    }

    public WALValue get(VersionedPartitionName versionedPartitionName, WALKey key) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).get(key);
    }

    public boolean containsKey(VersionedPartitionName versionedPartitionName, WALKey key) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).containsKey(key);
    }

    public <R> R takeRowUpdatesSince(VersionedPartitionName versionedPartitionName,
        long transactionId,
        PartitionStripe.TakeRowUpdates<R> takeRowUpdates)
        throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);

        PartitionStripe.RowStreamer streamer = rowStream -> partitionStore.takeRowUpdatesSince(transactionId, rowStream);
        return takeRowUpdates.give(versionedPartitionName, TxPartitionStatus.Status.ONLINE, streamer);
    }

    public boolean takeFromTransactionId(VersionedPartitionName versionedPartitionName, long transactionId, Highwaters highwaters,
        Scan<WALValue> scan)
        throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");
        return partitionIndex.get(versionedPartitionName).getWalStorage().takeFromTransactionId(transactionId, highwaters, scan);
    }

    public void rowScan(VersionedPartitionName versionedPartitionName, Scan<WALValue> scan) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            partitionIndex.get(versionedPartitionName).getWalStorage().rowScan(scan);
        }
    }

    public void rangeScan(VersionedPartitionName versionedPartitionName, WALKey from, WALKey to, Scan<WALValue> stream) throws Exception {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Must be a system partition");

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            partitionIndex.get(versionedPartitionName).getWalStorage().rangeScan(from, to, stream);
        }
    }

    public long count(VersionedPartitionName versionedPartitionName, WALStorage storage) throws Exception {
        return storage.count();
    }
}
