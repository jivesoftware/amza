package com.jivesoftware.os.amza.api.stream;

/**
 * @author jonathan.colt
 */
public interface UnprefixedWALKeys {

    boolean consume(UnprefixedWALKeyStream keyStream) throws Exception;

}
