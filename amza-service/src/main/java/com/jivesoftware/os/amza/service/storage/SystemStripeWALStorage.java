package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.service.storage.delta.StripeWALStorage;
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
public class SystemStripeWALStorage implements StripeWALStorage {

    @Override
    public void load(RegionIndex regionIndex) throws Exception {
    }

    @Override
    public void flush(boolean fsync) throws Exception {
    }

    @Override
    public void compact(RegionIndex regionIndex) throws Exception {
    }

    @Override
    public boolean expunge(VersionedRegionName versionedRegionName, WALStorage walStorage) throws Exception {
        return walStorage.expunge();
    }

    @Override
    public RowsChanged update(VersionedRegionName versionedRegionName,
        WALStorage storage,
        Commitable<WALValue> updates) throws Exception {

        return storage.update(false, updates);
    }

    @Override
    public WALValue get(VersionedRegionName versionedRegionName, WALStorage storage, WALKey key) throws Exception {
        return storage.get(key);
    }

    @Override
    public boolean containsKey(VersionedRegionName versionedRegionName, WALStorage storage, WALKey key) throws Exception {
        return storage.containsKey(key);
    }

    @Override
    public void takeRowUpdatesSince(VersionedRegionName versionedRegionName, WALStorage storage, long transactionId, RowStream rowUpdates)
        throws Exception {
        storage.takeRowUpdatesSince(transactionId, rowUpdates);
    }

    @Override
    public boolean takeFromTransactionId(VersionedRegionName versionedRegionName, WALStorage storage, long transactionId, Highwaters highwaters,
        Scan<WALValue> scan)
        throws Exception {
        return storage.takeFromTransactionId(transactionId, highwaters, scan);
    }

    @Override
    public void rangeScan(VersionedRegionName versionedRegionName, RangeScannable<WALValue> rangeScannable, WALKey from, WALKey to, Scan<WALValue> scan) throws
        Exception {
        rangeScannable.rangeScan(from, to, scan);
    }

    @Override
    public void rowScan(VersionedRegionName versionedRegionName, Scannable<WALValue> scanable, Scan<WALValue> scan) throws Exception {
        scanable.rowScan(scan);
    }

    @Override
    public long count(VersionedRegionName versionedRegionName, WALStorage storage) throws Exception {
        return storage.count();
    }
}
