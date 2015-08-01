package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;

/**
 * @author jonathan.colt
 */
public class NoOpWALIndex implements WALIndex {

    @Override
    public boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception {
        return pointers.consume((txId, prefix, key, timestamp, tombstoned, fp) -> {
            if (stream != null) {
                if (!stream.stream(WALMergeKeyPointerStream.ignored, txId, prefix, key, timestamp, tombstoned, fp)) {
                    return false;
                }
            }
            return true;
        });
    }

    @Override
    public boolean getPointer(byte[] prefix, byte[] key, WALKeyPointerStream stream) throws Exception {
        return stream.stream(prefix, key, -1, false, -1);
    }

    @Override
    public boolean getPointers(WALKeys keys, WALKeyPointerStream stream) throws Exception {
        return keys.consume((prefix, key) -> stream.stream(prefix, key, -1, false, -1));
    }

    @Override
    public boolean getPointers(KeyValues keyValues, KeyValuePointerStream stream) throws Exception {
        return keyValues.consume((prefix, key, value, valueTimestamp, valueTombstoned) ->
            stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, -1, false, -1));
    }

    @Override
    public boolean containsKeys(WALKeys keys, KeyContainedStream stream) throws Exception {
        return keys.consume((prefix, key) -> stream.stream(prefix, key, false));
    }

    @Override
    public boolean isEmpty() throws Exception {
        return false;
    }

    @Override
    public long size() throws Exception {
        return 0;
    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void compact() throws Exception {
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {
        return new NoOpCompactionWALIndex();
    }

    static class NoOpCompactionWALIndex implements CompactionWALIndex {

        @Override
        public boolean merge(TxKeyPointers pointers) throws Exception {
            return true;
        }

        @Override
        public void abort() throws Exception {
        }

        @Override
        public void commit() throws Exception {
        }
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
    public boolean delete() throws Exception {
        return true;
    }

}
