package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawPointers {

    boolean consume(RawPointerStream stream) throws Exception;
}
