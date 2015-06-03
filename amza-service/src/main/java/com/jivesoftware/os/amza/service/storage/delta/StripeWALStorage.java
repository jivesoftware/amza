package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.scan.RangeScannable;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.scan.Scannable;
import com.jivesoftware.os.amza.shared.region.VersionedRegionName;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorage;
import com.jivesoftware.os.amza.shared.wal.WALValue;

/**
 * @author jonathan.colt
 */
public interface StripeWALStorage {

    void load(RegionIndex regionIndex) throws Exception;

    void flush(boolean fsync) throws Exception;

    void compact(RegionIndex regionIndex) throws Exception;

    RowsChanged update(VersionedRegionName versionedRegionName,
        WALStorage storage,
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
