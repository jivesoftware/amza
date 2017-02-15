package com.jivesoftware.os.amza.api.stream;

/**
 * @author jonathan.colt
 */
public interface OffsetUnprefixedWALKeys {

    boolean consume(OffsetUnprefixedWALKeyStream keyStream) throws Exception;

}
