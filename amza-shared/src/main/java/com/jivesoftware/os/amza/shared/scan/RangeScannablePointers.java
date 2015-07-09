package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;

/**
 *
 * @author jonathan.colt
 */
public interface RangeScannablePointers extends ScannablePointers {

    boolean rangeScan(WALKey from, WALKey to, WALKeyPointerStream stream) throws Exception;
}
