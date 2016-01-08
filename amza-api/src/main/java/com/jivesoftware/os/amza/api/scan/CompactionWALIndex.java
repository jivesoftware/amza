package com.jivesoftware.os.amza.api.scan;

import com.jivesoftware.os.amza.api.stream.TxKeyPointers;
import java.util.concurrent.Callable;

/**
 * @author jonathan.colt
 */
public interface CompactionWALIndex {

    boolean merge(TxKeyPointers pointers) throws Exception;

    void commit(Callable<Void> commit) throws Exception;

}
