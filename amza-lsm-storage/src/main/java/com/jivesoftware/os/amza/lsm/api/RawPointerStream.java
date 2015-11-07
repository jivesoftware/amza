package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawPointerStream {

    boolean stream(byte[] rawEntry, int offset, int length) throws Exception;
}
