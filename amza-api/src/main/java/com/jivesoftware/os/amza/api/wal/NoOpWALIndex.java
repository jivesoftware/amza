package com.jivesoftware.os.amza.api.wal;

import com.jivesoftware.os.amza.api.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.api.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.scan.CompactionWALIndex;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.api.stream.KeyValues;
import com.jivesoftware.os.amza.api.stream.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.TxFpStream;
import com.jivesoftware.os.amza.api.stream.TxKeyPointers;
import com.jivesoftware.os.amza.api.stream.WALKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.WALKeyPointers;
import com.jivesoftware.os.amza.api.stream.WALMergeKeyPointerStream;
import java.util.concurrent.Callable;

/**
 * @author jonathan.colt
 */
public class NoOpWALIndex implements WALIndex {

    @Override
    public boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception {
        return pointers.consume((txId, prefix, key, timestamp, tombstoned, version, fp) -> {
            if (stream != null) {
                if (!stream.stream(WALMergeKeyPointerStream.ignored, txId, prefix, key, timestamp, tombstoned, version, fp)) {
                    return false;
                }
            }
            return true;
        });
    }

    @Override
    public boolean getPointer(byte[] prefix, byte[] key, WALKeyPointerStream stream) throws Exception {
        return stream.stream(prefix, key, -1, false, -1, -1);
    }

    @Override
    public boolean getPointers(byte[] prefix, UnprefixedWALKeys keys, WALKeyPointerStream stream) throws Exception {
        return keys.consume((key) -> stream.stream(prefix, key, -1, false, -1, -1));
    }

    @Override
    public boolean getPointers(KeyValues keyValues, KeyValuePointerStream stream) throws Exception {
        return keyValues.consume((rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion)
            -> stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, -1, false, -1, -1));
    }

    @Override
    public boolean containsKeys(byte[] prefix, UnprefixedWALKeys keys, KeyContainedStream stream) throws Exception {
        return keys.consume((key) -> stream.stream(prefix, key, false));
    }

    @Override
    public boolean isEmpty() throws Exception {
        return false;
    }

    @Override
    public long deltaCount(WALKeyPointers keyPointers) throws Exception {
        long[] delta = new long[1];
        boolean completed = keyPointers.consume((prefix, key, timestamp, tombstoned, version, fp) -> {
            if (!tombstoned) {
                delta[0]++;
            }
            return true;
        });
        if (!completed) {
            return -1;
        }
        return delta[0];
    }

//    @Override
//    public long size() throws Exception {
//        return 0;
//    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public CompactionWALIndex startCompaction(boolean hasActive) throws Exception {
        return new NoOpCompactionWALIndex();
    }

    static class NoOpCompactionWALIndex implements CompactionWALIndex {

        @Override
        public boolean merge(TxKeyPointers pointers) throws Exception {
            return true;
        }

        @Override
        public void commit(Callable<Void> commit) throws Exception {
        }
    }

    @Override
    public boolean takePrefixUpdatesSince(byte[] prefix, long sinceTransactionId, TxFpStream txFpStream) throws Exception {
        return true;
    }

    @Override
    public boolean rowScan(WALKeyPointerStream stream) throws Exception {
        return true;
    }

    @Override
    public boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, WALKeyPointerStream stream) throws Exception {
        return true;
    }

    @Override
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {
    }

    @Override
    public void delete() throws Exception {
    }

}
