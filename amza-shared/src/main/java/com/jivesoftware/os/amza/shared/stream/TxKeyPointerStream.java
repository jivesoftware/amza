package com.jivesoftware.os.amza.shared.stream;

/**
 *
 * @author jonathan.colt
 */
public interface TxKeyPointerStream {

    boolean stream(long txId, byte[] prefix, byte[] key, long timestamp, boolean tombstoned, long fp) throws Exception;

}
