package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyStream {

    boolean stream(WALKey key) throws Exception;

}
