package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class NoOpDeltaWALStorage implements DeltaWALStorage {

    @Override
    public void load(RegionProvider regionProvider) throws Exception {
    }

    @Override
    public void compact(RegionProvider regionProvider) throws Exception {
    }

    @Override
    public RowsChanged update(RegionName regionName, WALStorage storage, WALStorageUpdateMode upateMode, WALScanable rowUpdates) throws Exception {
        return storage.update(null, upateMode, rowUpdates);
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
    public List<WALValue> get(RegionName regionName, WALStorage storage, List<WALKey> keys) throws Exception {
        return storage.get(keys);
    }

    @Override
    public List<Boolean> containsKey(RegionName regionName, WALStorage storage, List<WALKey> keys) throws Exception {
        return storage.containsKey(keys);
    }

    @Override
    public void takeRowUpdatesSince(RegionName regionName, WALStorage storage, long transactionId, RowStream rowUpdates) throws Exception {
        storage.takeRowUpdatesSince(transactionId, rowUpdates);
    }

    @Override
    public void rangeScan(RegionName regionName, RangeScannable rangeScannable, WALKey from, WALKey to, WALScan walScan) throws Exception {
        rangeScannable.rangeScan(from, to, walScan);
    }

    @Override
    public void rowScan(RegionName regionName, WALScanable scanable, WALScan walScan) throws Exception {
        scanable.rowScan(walScan);
    }

    @Override
    public long size(RegionName regionName, WALStorage storage) throws Exception {
        return storage.size();
    }

}
