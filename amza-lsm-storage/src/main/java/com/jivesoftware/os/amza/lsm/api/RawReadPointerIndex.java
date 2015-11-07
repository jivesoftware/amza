package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawReadPointerIndex {

    RawPointGet getPointer() throws Exception;

    RawNextPointer rangeScan(byte[] from, byte[] to) throws Exception;

    RawNextPointer rowScan() throws Exception;

    void close() throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}
