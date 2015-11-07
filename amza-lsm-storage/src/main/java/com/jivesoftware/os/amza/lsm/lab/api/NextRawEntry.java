package com.jivesoftware.os.amza.lsm.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface NextRawEntry {

    boolean next(RawEntryStream stream) throws Exception;
}
