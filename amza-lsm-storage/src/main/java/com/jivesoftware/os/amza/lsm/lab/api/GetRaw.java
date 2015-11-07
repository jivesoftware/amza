package com.jivesoftware.os.amza.lsm.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface GetRaw {

    boolean next(byte[] key, RawEntryStream stream) throws Exception;
}
