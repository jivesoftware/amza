package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.stream.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.shared.stream.TxKeyPointers;
import com.jivesoftware.os.amza.shared.stream.WALKeyPointerStream;

/**
 *
 * @author jonathan.colt
 */
public interface CompactableWALIndex {

    CompactionWALIndex startCompaction() throws Exception;

    boolean getPointer(byte[] prefix, byte[] key, WALKeyPointerStream stream) throws Exception;

    boolean isEmpty() throws Exception;

    boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception;

    /**
     * Force persistence of all changes
     * @throws java.lang.Exception
     */
    void commit() throws Exception;
}
