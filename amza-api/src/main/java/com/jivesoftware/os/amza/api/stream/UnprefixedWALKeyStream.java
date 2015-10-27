package com.jivesoftware.os.amza.api.stream;

/**
 * @author jonathan.colt
 */
public interface UnprefixedWALKeyStream {

    boolean stream(byte[] key) throws Exception;

}
