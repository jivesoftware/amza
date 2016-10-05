package com.jivesoftware.os.amza.api.wal;

import com.jivesoftware.os.amza.api.scan.CompactionWALIndex;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.api.stream.KeyValues;
import com.jivesoftware.os.amza.api.stream.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.TxFpStream;
import com.jivesoftware.os.amza.api.stream.TxKeyPointers;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.WALKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.WALKeyPointers;
import com.jivesoftware.os.amza.api.stream.WALMergeKeyPointerStream;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author jonathan.colt
 */
public class NoOpWALIndex implements WALIndex {

    private final String providerName;
    private volatile int currentStripe;

    public NoOpWALIndex(String providerName, int currentStripe) {
        this.providerName = providerName;
        this.currentStripe = currentStripe;
    }

    @Override
    public int getStripe() {
        return currentStripe;
    }

    @Override
    public String getProviderName() {
        return providerName;
    }

    @Override
    public boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception {
        return pointers.consume((txId, prefix, key, value, timestamp, tombstoned, version, fp) -> {
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
        return stream.stream(prefix, key, -1, false, -1, -1, false, null);
    }

    @Override
    public boolean getPointers(byte[] prefix, UnprefixedWALKeys keys, WALKeyPointerStream stream) throws Exception {
        return keys.consume((key) -> stream.stream(prefix, key, -1, false, -1, -1, false, null));
    }

    @Override
    public boolean getPointers(KeyValues keyValues, KeyValuePointerStream stream) throws Exception {
        return keyValues.consume((prefix, key, value, valueTimestamp, valueTombstoned, valueVersion)
            -> stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, -1, false, -1, -1, false, null));
    }

    @Override
    public boolean containsKeys(byte[] prefix, UnprefixedWALKeys keys, KeyContainedStream stream) throws Exception {
        return keys.consume((key) -> stream.stream(prefix, key, false, -1, -1));
    }

    @Override
    public boolean exists() throws Exception {
        return true;
    }

    @Override
    public long deltaCount(WALKeyPointers keyPointers) throws Exception {
        long[] delta = new long[1];
        boolean completed = keyPointers.consume((prefix, key, timestamp, tombstoned, version, fp, hasValue, value) -> {
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
    public void commit(boolean fsync) throws Exception {
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public CompactionWALIndex startCompaction(boolean hasActive, int compactionStripe) throws Exception {
        return new CompactionWALIndex() {

            @Override
            public boolean merge(TxKeyPointers pointers) throws Exception {
                return true;
            }

            @Override
            public void commit(boolean fsync, Callable<Void> commit) throws Exception {
                currentStripe = compactionStripe;
            }

            @Override
            public void abort() throws Exception {
            }
        };
    }

    @Override
    public boolean takePrefixUpdatesSince(byte[] prefix, long sinceTransactionId, TxFpStream txFpStream) throws Exception {
        return true;
    }

    @Override
    public boolean rowScan(WALKeyPointerStream stream, boolean hydrateValues) throws Exception {
        return true;
    }

    @Override
    public boolean rangeScan(byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        WALKeyPointerStream stream,
        boolean hydrateValues) throws Exception {
        return true;
    }

    @Override
    public void updatedProperties(Map<String, String> properties) {
    }

    @Override
    public void delete() throws Exception {
    }

}
