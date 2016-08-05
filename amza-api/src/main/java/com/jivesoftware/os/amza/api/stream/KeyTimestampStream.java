package com.jivesoftware.os.amza.api.stream;

/**
 *
 * @author jonathan.colt
 */
public interface KeyTimestampStream {

    boolean stream(byte[] prefix, byte[] key, long timestamp, long version) throws Exception;
}
