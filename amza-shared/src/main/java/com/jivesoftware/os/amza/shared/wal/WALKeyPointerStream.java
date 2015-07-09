package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyPointerStream {

    boolean stream(byte[] key, long timestamp, boolean tombstoned, long fp) throws Exception;

}
