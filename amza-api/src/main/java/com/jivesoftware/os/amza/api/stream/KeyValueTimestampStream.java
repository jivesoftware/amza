package com.jivesoftware.os.amza.api.stream;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValueTimestampStream {

    boolean stream(byte[] prefix, byte[] key, byte[] value, long timestamp) throws Exception;
}
