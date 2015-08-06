package com.jivesoftware.os.amza.lsm;

/**
 *
 * @author jonathan.colt
 */
public interface ReadablePointerIndex {

    NextPointer getPointer(byte[] key) throws Exception;

    NextPointer rangeScan(byte[] from, byte[] to) throws Exception;

    NextPointer rowScan() throws Exception;

}
