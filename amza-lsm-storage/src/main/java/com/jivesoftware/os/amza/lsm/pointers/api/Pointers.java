package com.jivesoftware.os.amza.lsm.pointers.api;

/**
 *
 * @author jonathan.colt
 */
public interface Pointers {

    boolean consume(PointerStream stream) throws Exception;
}
