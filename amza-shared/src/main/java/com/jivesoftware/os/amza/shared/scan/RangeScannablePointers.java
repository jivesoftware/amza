package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;

/**
 *
 * @author jonathan.colt
 */
public interface RangeScannablePointers extends ScannablePointers {

    boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception;
}
