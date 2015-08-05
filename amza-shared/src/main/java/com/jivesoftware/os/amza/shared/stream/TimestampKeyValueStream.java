package com.jivesoftware.os.amza.shared.stream;

/**
 *
 * @author jonathan.colt
 */
public interface TimestampKeyValueStream {

    boolean stream(byte[] prefix, byte[] key, byte[] value, long timestamp) throws Exception;
}
