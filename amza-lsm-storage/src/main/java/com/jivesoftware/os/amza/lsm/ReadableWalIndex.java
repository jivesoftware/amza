package com.jivesoftware.os.amza.lsm;

/**
 *
 * @author jonathan.colt
 */
public interface ReadableWalIndex {

    FeedNext getPointer(byte[] key) throws Exception;

    FeedNext rangeScan(byte[] from, byte[] to) throws Exception;

    FeedNext rowScan() throws Exception;

}
