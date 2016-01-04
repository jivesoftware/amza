package com.jivesoftware.os.amza.api.scan;

import com.jivesoftware.os.amza.api.stream.TxKeyPointers;

/**
 *
 * @author jonathan.colt
 */
public interface CompactionWALIndex {

    boolean merge(TxKeyPointers pointers) throws Exception;

    void abort() throws Exception;

    void commit() throws Exception;

}
