package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.stream.KeyValueStream;

/**
 * @author jonathan.colt
 */
public interface RangeScannable extends Scannable {

    boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, KeyValueStream keyValueStream) throws Exception;
}
