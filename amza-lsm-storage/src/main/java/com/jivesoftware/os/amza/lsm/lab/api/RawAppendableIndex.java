package com.jivesoftware.os.amza.lsm.lab.api;

import com.jivesoftware.os.amza.lsm.lab.IndexRangeId;

/**
 *
 * @author jonathan.colt
 */
public interface RawAppendableIndex {

    IndexRangeId id();

    boolean append(RawEntries entries) throws Exception;

    void close() throws Exception;
}
