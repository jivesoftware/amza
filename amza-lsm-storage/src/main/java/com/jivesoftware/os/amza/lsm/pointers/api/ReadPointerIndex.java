package com.jivesoftware.os.amza.lsm.pointers.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadPointerIndex {

    interface Tx<R> {

        R tx(NextPointer nextPointer) throws Exception;
    }

    <R> R getPointer(byte[] key, Tx<R> tx) throws Exception;

    <R> R rangeScan(byte[] from, byte[] to, Tx<R> tx) throws Exception;

    <R> R rowScan( Tx<R> tx) throws Exception;

    void close() throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}
