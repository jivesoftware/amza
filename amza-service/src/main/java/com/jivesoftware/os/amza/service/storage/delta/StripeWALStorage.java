package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.service.storage.RegionIndex;
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
public interface StripeWALStorage {

    void load(RegionIndex regionIndex) throws Exception;

    void flush(boolean fsync) throws Exception;

    void compact(RegionIndex regionIndex) throws Exception;

    RowsChanged update(RegionName regionName,
        WALStorage storage,
        WALReplicator replicator,
        WALStorageUpdateMode mode,
        Scannable<WALValue> updates) throws Exception;

    WALValue get(RegionName regionName, WALStorage storage, WALKey key) throws Exception;

    boolean containsKey(RegionName regionName, WALStorage storage, WALKey key) throws Exception;

    void takeRowUpdatesSince(RegionName regionName, WALStorage storage, final long transactionId, RowStream rowUpdates) throws Exception;

    boolean takeFromTransactionId(RegionName regionName, WALStorage walStorage, long transactionId, Scan<WALValue> scan) throws Exception;

    void rangeScan(RegionName regionName, RangeScannable<WALValue> rangeScannable, WALKey from, WALKey to, Scan<WALValue> scan) throws Exception;

    void rowScan(RegionName regionName, Scannable<WALValue> scanable, Scan<WALValue> scan) throws Exception;

    long count(RegionName regionName, WALStorage storage) throws Exception;
}
