package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface NextPointer {

    boolean next(PointerStream stream) throws Exception;
}
