package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKey;

/**
 *
 * @author jonathan.colt
 */
public interface RangeScannablePointers extends ScannablePointers {

    void rangeScan(WALKey from, WALKey to, WALKeyPointerStream stream) throws Exception;
}
