package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValueStream {

    boolean stream(WALKey key, WALValue value) throws Exception;

}
