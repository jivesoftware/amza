package com.jivesoftware.os.amza.lsm;

/**
 *
 * @author jonathan.colt
 */
public interface PointerStream {

    boolean stream(int sortIndex, byte[] key, long timestamp, boolean tombstoned, long pointer) throws Exception;
}
