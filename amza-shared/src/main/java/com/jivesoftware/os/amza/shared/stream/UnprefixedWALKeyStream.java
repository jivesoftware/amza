package com.jivesoftware.os.amza.shared.stream;

/**
 * @author jonathan.colt
 */
public interface UnprefixedWALKeyStream {

    boolean stream(byte[] key) throws Exception;

}
