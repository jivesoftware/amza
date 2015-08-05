package com.jivesoftware.os.amza.shared.stream;

/**
 * @author jonathan.colt
 */
public interface UnprefixedWALKeys {

    boolean consume(UnprefixedWALKeyStream keyStream) throws Exception;

}
