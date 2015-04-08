package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;

/**
 * @author jonathan.colt
 */
public interface DeltaWALStorage {

    void load(RegionProvider regionProvider) throws Exception;

    void compact(RegionProvider regionProvider) throws Exception;

    RowsChanged update(RegionName regionName, WALStorage storage, WALStorageUpdateMode upateMode, Scannable<WALValue> rowUpdates) throws Exception;

    WALValue get(RegionName regionName, WALStorage storage, WALKey key) throws Exception;

    boolean containsKey(RegionName regionName, WALStorage storage, WALKey key) throws Exception;

    void takeRowUpdatesSince(RegionName regionName, WALStorage storage, final long transactionId, RowStream rowUpdates) throws Exception;

    void rangeScan(RegionName regionName, RangeScannable<WALValue> rangeScannable, WALKey from, WALKey to, Scan<WALValue> scan) throws Exception;

    void rowScan(RegionName regionName, Scannable<WALValue> scanable, Scan<WALValue> scan) throws Exception;

    long count(RegionName regionName, WALStorage storage) throws Exception;
}
