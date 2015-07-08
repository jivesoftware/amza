package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyValuePointerStream {

    boolean stream(WALKey key, WALValue value, long timestamp, boolean tombstoned, long fp) throws Exception;

}
