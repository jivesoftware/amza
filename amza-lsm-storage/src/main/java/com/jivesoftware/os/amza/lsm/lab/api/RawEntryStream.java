package com.jivesoftware.os.amza.lsm.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntryStream {

    boolean stream(byte[] rawEntry, int offset, int length) throws Exception;
}
