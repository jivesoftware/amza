package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.WALKey;

/**
 *
 * @author jonathan.colt
 */
public interface RangeScannable extends Scannable {

    boolean rangeScan(byte[] from, byte[] to, KeyValueStream scan) throws Exception;
}
