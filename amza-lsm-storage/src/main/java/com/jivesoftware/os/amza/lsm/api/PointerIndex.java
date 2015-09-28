package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface PointerIndex {

    NextPointer getPointer(byte[] key) throws Exception;

    NextPointer rangeScan(byte[] from, byte[] to) throws Exception;

    NextPointer rowScan() throws Exception;

}
