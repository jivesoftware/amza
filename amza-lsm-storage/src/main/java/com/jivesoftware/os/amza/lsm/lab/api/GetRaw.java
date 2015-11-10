package com.jivesoftware.os.amza.lsm.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface GetRaw {

    boolean get(byte[] key, RawEntryStream stream) throws Exception;

    boolean result();
}
