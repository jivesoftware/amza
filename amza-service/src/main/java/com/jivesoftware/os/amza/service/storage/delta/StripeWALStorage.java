package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RangeScannable;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.scan.Scannable;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorage;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;

/**
 * @author jonathan.colt
 */
public interface StripeWALStorage {

    void load(PartitionIndex partitionIndex) throws Exception;

    void flush(boolean fsync) throws Exception;

    void compact(PartitionIndex partitionIndex, boolean force) throws Exception;

    RowsChanged update(HighwaterStorage highwaterStorage,
        VersionedPartitionName versionedPartitionName,
        Status partitionStatus,
        WALStorage storage,
        Commitable<WALValue> updates,
        WALUpdated updated) throws Exception;

    long getHighestTxId(VersionedPartitionName versionedPartitionName,
        WALStorage storage) throws Exception;

    WALValue get(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        WALKey key) throws Exception;

    boolean containsKey(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        WALKey key) throws Exception;

    void takeRowUpdatesSince(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        long transactionId,
        RowStream rowUpdates) throws Exception;

    boolean takeFromTransactionId(VersionedPartitionName versionedPartitionName,
        WALStorage walStorage,
        long transactionId,
        Highwaters highwaters,
        Scan<WALValue> scan) throws Exception;

    void rangeScan(VersionedPartitionName versionedPartitionName,
        RangeScannable<WALValue> rangeScannable,
        WALKey from,
        WALKey to,
        Scan<WALValue> scan) throws Exception;

    void rowScan(VersionedPartitionName versionedPartitionName,
        Scannable<WALValue> scanable,
        Scan<WALValue> scan) throws Exception;

    long count(VersionedPartitionName versionedPartitionName,
        WALStorage storage) throws Exception;

    boolean expunge(VersionedPartitionName versionedPartitionName,
        WALStorage walStorage) throws Exception;
}
