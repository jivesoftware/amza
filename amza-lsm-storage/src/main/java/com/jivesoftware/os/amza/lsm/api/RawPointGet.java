package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawPointGet {

    boolean next(byte[] key, RawPointerStream stream) throws Exception;
}
