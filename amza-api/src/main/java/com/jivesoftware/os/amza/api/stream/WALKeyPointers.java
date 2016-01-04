package com.jivesoftware.os.amza.api.stream;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyPointers {

    boolean consume(WALKeyPointerStream stream) throws Exception;

}
