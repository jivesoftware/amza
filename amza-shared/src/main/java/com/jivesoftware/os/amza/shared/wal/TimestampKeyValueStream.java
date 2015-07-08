package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface TimestampKeyValueStream {

    boolean stream(WALKey key, byte[] value, long timestamp) throws Exception;
}
