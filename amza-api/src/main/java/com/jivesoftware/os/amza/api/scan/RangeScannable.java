package com.jivesoftware.os.amza.api.scan;

import com.jivesoftware.os.amza.api.stream.KeyValueStream;

/**
 * @author jonathan.colt
 */
public interface RangeScannable extends Scannable {

    boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, KeyValueStream keyValueStream, boolean hydrateValues) throws Exception;
}
