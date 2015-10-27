package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadPointerIndex {

    NextPointer getPointer(byte[] key) throws Exception;

    NextPointer rangeScan(byte[] from, byte[] to) throws Exception;

    NextPointer rowScan() throws Exception;

    void close() throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}
