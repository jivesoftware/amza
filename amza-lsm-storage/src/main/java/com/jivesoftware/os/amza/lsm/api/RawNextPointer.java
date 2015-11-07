package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawNextPointer {

    boolean next(RawPointerStream stream) throws Exception;
}
