package com.jivesoftware.os.amza.api.stream;

/**
 *
 */
public interface TxFpStream {

    boolean stream(long txId, long fp, boolean hasValue, byte[] value) throws Exception;

}
