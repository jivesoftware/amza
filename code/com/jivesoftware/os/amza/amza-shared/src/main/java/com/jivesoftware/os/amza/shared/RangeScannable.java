package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public interface RangeScannable extends WALScanable {

    void rangeScan(WALKey from, WALKey to, WALScan walScan) throws Exception;
}
