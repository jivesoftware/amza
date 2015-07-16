package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.wal.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointers;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;

/**
 *
 * @author jonathan.colt
 */
public interface CompactableWALIndex {

    CompactionWALIndex startCompaction() throws Exception;

    interface CompactionWALIndex {

        boolean merge(TxKeyPointers pointers) throws Exception;

        void abort() throws Exception;

        void commit() throws Exception;
    }

    boolean getPointer(byte[] key, WALKeyPointerStream stream) throws Exception;

    boolean isEmpty() throws Exception;

    boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception;

    /**
     * Force persistence of all changes
     * @throws java.lang.Exception
     */
    void commit() throws Exception;
}
