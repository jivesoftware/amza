package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyPointers {

    boolean consume(WALKeyPointerStream stream) throws Exception;

}
