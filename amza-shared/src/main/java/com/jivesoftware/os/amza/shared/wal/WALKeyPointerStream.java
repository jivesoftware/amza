package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyPointerStream {

    boolean stream(WALKey key, long timestamp, boolean tombstoned, long fp) throws Exception;

}
