package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyPointers {

    void consume(WALKeyPointerStream stream) throws Exception;

}
