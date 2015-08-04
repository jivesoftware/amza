package com.jivesoftware.os.amza.shared.wal;

/**
 *
 */
public interface TxFpStream {

    boolean stream(long txId, long fp) throws Exception;

}
