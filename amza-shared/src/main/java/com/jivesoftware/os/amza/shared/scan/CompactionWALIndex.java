package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.stream.TxKeyPointers;

/**
 *
 * @author jonathan.colt
 */
public interface CompactionWALIndex {

    boolean merge(TxKeyPointers pointers) throws Exception;

    void abort() throws Exception;

    void commit() throws Exception;

}
