package com.jivesoftware.os.amza.lsm.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawAppendableIndex {

    boolean append(RawEntries entries) throws Exception;

    void commit() throws Exception;
}
