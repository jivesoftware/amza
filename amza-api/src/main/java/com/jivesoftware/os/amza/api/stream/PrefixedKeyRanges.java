package com.jivesoftware.os.amza.api.stream;

/**
 * @author jonathan.colt
 */
public interface PrefixedKeyRanges {

    boolean consume(PrefixedKeyRangeStream stream) throws Exception;

}
