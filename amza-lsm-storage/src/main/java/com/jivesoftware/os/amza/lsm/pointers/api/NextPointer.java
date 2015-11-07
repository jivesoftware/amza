package com.jivesoftware.os.amza.lsm.pointers.api;

/**
 *
 * @author jonathan.colt
 */
public interface NextPointer {

    boolean next(PointerStream stream) throws Exception;
}
