package com.jivesoftware.os.amza.lsm.pointers.api;

/**
 *
 * @author jonathan.colt
 */
public interface PointerStream {

    boolean stream(byte[] key, long timestamp, boolean tombstoned, long version, long pointer) throws Exception;
}
