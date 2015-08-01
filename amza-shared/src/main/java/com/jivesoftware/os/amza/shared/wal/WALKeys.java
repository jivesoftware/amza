package com.jivesoftware.os.amza.shared.wal;

/**
 * @author jonathan.colt
 */
public interface WALKeys {

    boolean consume(WALKeyStream keyStream) throws Exception;

}
