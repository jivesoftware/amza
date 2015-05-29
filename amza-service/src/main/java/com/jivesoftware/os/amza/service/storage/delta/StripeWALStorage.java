package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.shared.Commitable;
import com.jivesoftware.os.amza.shared.Highwaters;
import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;

/**
 * @author jonathan.colt
 */
public interface StripeWALStorage {

    void load(RegionIndex regionIndex) throws Exception;

    void flush(boolean fsync) throws Exception;

    void compact(RegionIndex regionIndex) throws Exception;

    RowsChanged update(VersionedRegionName versionedRegionName,
        WALStorage storage,
        WALReplicator replicator,
        WALStorageUpdateMode mode,
        Commitable<WALValue> updates) throws Exception;

    WALValue get(VersionedRegionName versionedRegionName,
        WALStorage storage,
        WALKey key) throws Exception;

    boolean containsKey(VersionedRegionName versionedRegionName,
        WALStorage storage,
        WALKey key) throws Exception;

    void takeRowUpdatesSince(VersionedRegionName versionedRegionName,
        WALStorage storage,
        long transactionId,
        RowStream rowUpdates) throws Exception;

    boolean takeFromTransactionId(VersionedRegionName versionedRegionName,
        WALStorage walStorage,
        long transactionId,
        Highwaters highwaters,
        Scan<WALValue> scan) throws Exception;

    void rangeScan(VersionedRegionName versionedRegionName,
        RangeScannable<WALValue> rangeScannable,
        WALKey from,
        WALKey to,
        Scan<WALValue> scan) throws Exception;

    void rowScan(VersionedRegionName versionedRegionName,
        Scannable<WALValue> scanable,
        Scan<WALValue> scan) throws Exception;

    long count(VersionedRegionName versionedRegionName,
        WALStorage storage) throws Exception;

    boolean expunge(VersionedRegionName versionedRegionName,
        WALStorage walStorage) throws Exception;
}
