package com.jivesoftware.os.amza.lsm.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntries {

    boolean consume(RawEntryStream stream) throws Exception;
}
