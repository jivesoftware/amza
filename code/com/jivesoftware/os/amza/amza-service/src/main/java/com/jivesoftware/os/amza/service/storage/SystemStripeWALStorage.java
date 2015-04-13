package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.service.storage.delta.StripeWALStorage;
import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;

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
    public RowsChanged update(RegionName regionName,
        WALStorage storage,
        WALReplicator replicator,
        WALStorageUpdateMode mode,
        Scannable<WALValue> updates) throws Exception {

        return storage.update(null, replicator, mode, updates);
    }

    @Override
    public WALValue get(RegionName regionName, WALStorage storage, WALKey key) throws Exception {
        return storage.get(key);
    }

    @Override
    public boolean containsKey(RegionName regionName, WALStorage storage, WALKey key) throws Exception {
        return storage.containsKey(key);
    }

    @Override
    public void takeRowUpdatesSince(RegionName regionName, WALStorage storage, long transactionId, RowStream rowUpdates) throws Exception {
        storage.takeRowUpdatesSince(transactionId, rowUpdates);
    }

    @Override
    public void rangeScan(RegionName regionName, RangeScannable<WALValue> rangeScannable, WALKey from, WALKey to, Scan<WALValue> scan) throws Exception {
        rangeScannable.rangeScan(from, to, scan);
    }

    @Override
    public void rowScan(RegionName regionName, Scannable<WALValue> scanable, Scan<WALValue> scan) throws Exception {
        scanable.rowScan(scan);
    }

    @Override
    public long count(RegionName regionName, WALStorage storage) throws Exception {
        return storage.count();
    }
}
