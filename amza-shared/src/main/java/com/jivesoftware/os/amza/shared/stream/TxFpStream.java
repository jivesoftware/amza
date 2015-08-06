package com.jivesoftware.os.amza.shared.stream;

/**
 *
 */
public interface TxFpStream {

    boolean stream(long txId, long fp) throws Exception;

}
