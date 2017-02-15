package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.OffsetUnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.aquarium.LivelyEndState;

/**
 * @author jonathan.colt
 */
public interface Partition {

    void commit(Consistency consistency,
        byte[] prefix,
        ClientUpdates updates,
        long timeoutInMillis) throws Exception;

    boolean get(Consistency consistency, byte[] prefix, boolean requiresOnline, UnprefixedWALKeys keys, KeyValueStream stream) throws Exception;

    boolean scan(Iterable<ScanRange> ranges, boolean hydrateValues, boolean requiresOnline, KeyValueStream stream) throws Exception;

    TakeResult takeFromTransactionId(long txId,
        boolean requiresOnline,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception;

    TakeResult takePrefixFromTransactionId(byte[] prefix,
        long txId,
        boolean requiresOnline,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception;

    // TODO fix or deprecate: Currently known to be broken. Only accurate if you never delete.
    long count() throws Exception;

    /**
     * Inaccurate by the number of unmerged changes on the delta WAL
     */
    long approximateCount() throws Exception;

    long highestTxId() throws Exception;

    LivelyEndState livelyEndState() throws Exception;

    class ScanRange {

        public static final ScanRange ROW_SCAN = new ScanRange(null, null, null, null);

        public final byte[] fromPrefix;
        public final byte[] fromKey;
        public final byte[] toPrefix;
        public final byte[] toKey;

        /**
         * @param fromPrefix nullable (inclusive)
         * @param fromKey    nullable (inclusive)
         * @param toPrefix   nullable (exclusive)
         * @param toKey      nullable (exclusive)
         */
        public ScanRange(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey) {
            this.fromPrefix = fromPrefix;
            this.fromKey = fromKey;
            this.toPrefix = toPrefix;
            this.toKey = toKey;
        }
    }

}
