package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;

/**
 * @author jonathan.colt
 */
public class NoOpWALIndex implements WALIndex {

    @Override
    public boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception {
        return pointers.consume((long txId, byte[] key, long timestamp, boolean tombstoned, long fp) -> {
            if (stream != null) {
                if (!stream.stream(WALMergeKeyPointerStream.ignored, txId, key, timestamp, tombstoned, fp)) {
                    return false;
                }
            }
            return true;
        });
    }

    @Override
    public boolean getPointer(byte[] key, WALKeyPointerStream stream) throws Exception {
        return stream.stream(key, -1, false, -1);
    }

    @Override
    public boolean getPointers(WALKeys keys, WALKeyPointerStream stream) throws Exception {
        return keys.consume(key -> stream.stream(key, -1, false, -1));
    }

    @Override
    public boolean getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        return keyValues.consume((key, value, valueTimestamp, valueTombstoned) ->
            stream.stream(key, value, valueTimestamp, valueTombstoned, -1, false, -1));
    }

    @Override
    public boolean containsKeys(WALKeys keys, KeyContainedStream stream) throws Exception {
        return keys.consume(key -> stream.stream(key, false));
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
    public boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
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
