package com.jivesoftware.os.amza.shared.wal;

/**
 * @author jonathan.colt
 */
public interface WALKeyStream {

    boolean stream(byte[] key) throws Exception;

}
