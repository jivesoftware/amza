package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeys {

    void consume(WALKeyPointerStream stream) throws Exception;

}
