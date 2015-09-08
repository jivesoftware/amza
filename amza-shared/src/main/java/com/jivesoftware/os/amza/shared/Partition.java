package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.shared.stream.KeyValueStream;

/**
 *
 * @author jonathan.colt
 */
public interface Partition {

    void commit(Consistency consistency, byte[] prefix,
        Commitable updates,
        long timeoutInMillis) throws Exception;

    boolean get(Consistency consistency, byte[] prefix, UnprefixedWALKeys keys, KeyValueStream stream) throws Exception;

    /**
     * @param fromPrefix   nullable (inclusive)
     * @param fromKey      nullable (inclusive)
     * @param toPrefix     nullable (exclusive)
     * @param toKey        nullable (exclusive)
     * @param scan
     * @throws Exception
     */
    boolean scan(Consistency consistency,
        byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        KeyValueTimestampStream scan) throws Exception;

    TakeResult takeFromTransactionId(Consistency consistency,
        long txId,
        Highwaters highwaters,
        TxKeyValueStream stream) throws
        Exception;

    TakeResult takePrefixFromTransactionId(Consistency consistency,
        byte[] prefix,
        long txId,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception;

    // TODO fix or deprecate: Currently know to be broken. Only accurate if you never delete.
    long count() throws Exception;

    <R> R highestTxId(HighestPartitionTx<R> highestPartitionTx) throws Exception;

}
