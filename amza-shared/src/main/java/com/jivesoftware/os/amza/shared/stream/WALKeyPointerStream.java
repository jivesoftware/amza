package com.jivesoftware.os.amza.shared.stream;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyPointerStream {

    boolean stream(byte[] prefix, byte[] key, long timestamp, boolean tombstoned, long fp) throws Exception;

}
