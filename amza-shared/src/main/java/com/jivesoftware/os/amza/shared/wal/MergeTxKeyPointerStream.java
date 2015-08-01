package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface MergeTxKeyPointerStream {

    static byte added = 1;
    static byte clobbered = 2;
    static byte ignored = 3;

    boolean stream(byte mode, long txId, byte[] prefix, byte[] key, long timestamp, boolean tombstoned, long fp) throws Exception;

}
