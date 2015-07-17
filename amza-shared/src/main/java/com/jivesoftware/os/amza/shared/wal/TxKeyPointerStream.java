package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface TxKeyPointerStream {

    boolean stream(long txId, byte[] key, long timestamp, boolean tombstoned, long fp) throws Exception;

}
