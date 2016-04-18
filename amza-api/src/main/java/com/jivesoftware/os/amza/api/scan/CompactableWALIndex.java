package com.jivesoftware.os.amza.api.scan;

import com.jivesoftware.os.amza.api.stream.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.TxKeyPointers;
import com.jivesoftware.os.amza.api.stream.WALKeyPointerStream;

/**
 *
 * @author jonathan.colt
 */
public interface CompactableWALIndex {

    int getStripe();

    CompactionWALIndex startCompaction(boolean hasActive, int stripe) throws Exception;

    boolean getPointer(byte[] prefix, byte[] key, WALKeyPointerStream stream) throws Exception;

    boolean isEmpty() throws Exception;

    boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception;

    /**
     * Force persistence of all changes
     * @throws java.lang.Exception
     */
    void commit(boolean fsync) throws Exception;
}
