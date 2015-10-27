package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface Pointers {

    boolean consume(PointerStream stream) throws Exception;
}
