package com.jivesoftware.os.amza.api.stream;

/**
 * @author jonathan.colt
 */
public interface OffsetUnprefixedWALKeyStream {

    boolean stream(byte[] key, int offset, int length) throws Exception;

}
