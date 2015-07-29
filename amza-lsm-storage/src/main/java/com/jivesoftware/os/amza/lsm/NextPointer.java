package com.jivesoftware.os.amza.lsm;

/**
 *
 * @author jonathan.colt
 */
public interface NextPointer {

    boolean next(PointerStream stream) throws Exception;
}
