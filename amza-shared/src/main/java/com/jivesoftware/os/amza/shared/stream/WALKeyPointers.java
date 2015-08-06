package com.jivesoftware.os.amza.shared.stream;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyPointers {

    boolean consume(WALKeyPointerStream stream) throws Exception;

}
