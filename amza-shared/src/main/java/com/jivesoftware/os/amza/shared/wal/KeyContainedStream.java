package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface KeyContainedStream {

    boolean stream(byte[] key, boolean contained) throws Exception;

}
