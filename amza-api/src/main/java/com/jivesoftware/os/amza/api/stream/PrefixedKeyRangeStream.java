package com.jivesoftware.os.amza.api.stream;

/**
 * @author jonathan.colt
 */
public interface PrefixedKeyRangeStream {

    boolean stream(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey) throws Exception;

}
