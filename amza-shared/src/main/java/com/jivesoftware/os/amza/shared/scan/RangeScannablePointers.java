package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.stream.WALKeyPointerStream;

/**
 *
 * @author jonathan.colt
 */
public interface RangeScannablePointers extends ScannablePointers {

    boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, WALKeyPointerStream stream) throws Exception;
}
