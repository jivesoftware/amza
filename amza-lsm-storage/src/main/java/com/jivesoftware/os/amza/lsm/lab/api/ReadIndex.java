package com.jivesoftware.os.amza.lsm.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadIndex {

    void acquire() throws Exception;

    void release();

    GetRaw get() throws Exception;

    NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception;

    NextRawEntry rowScan() throws Exception;

    void close() throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}
