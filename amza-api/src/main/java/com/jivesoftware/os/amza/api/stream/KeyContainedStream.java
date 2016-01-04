package com.jivesoftware.os.amza.api.stream;

/**
 *
 * @author jonathan.colt
 */
public interface KeyContainedStream {

    boolean stream(byte[] prefix, byte[] key, boolean contained) throws Exception;

}
