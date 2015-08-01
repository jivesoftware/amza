package com.jivesoftware.os.amza.shared.wal;

/**
 * @author jonathan.colt
 */
public interface WALKeyStream {

    boolean stream(byte[] prefix, byte[] key) throws Exception;

}
