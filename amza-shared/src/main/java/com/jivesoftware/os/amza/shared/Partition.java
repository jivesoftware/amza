package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.scan.Commitable;
import com.jivesoftware.os.amza.api.scan.Scan;
import com.jivesoftware.os.amza.api.stream.TimestampKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;

/**
 *
 * @author jonathan.colt
 */
public interface Partition {

    void commit(Consistency consistency, byte[] prefix,
        Commitable updates,
        long timeoutInMillis) throws Exception;

    boolean get(Consistency consistency, byte[] prefix, UnprefixedWALKeys keys, TimestampKeyValueStream valuesStream) throws Exception;

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
        Scan scan) throws Exception;

    TakeResult takeFromTransactionId(Consistency consistency,
        long txId,
        Highwaters highwaters,
        Scan scan) throws
        Exception;

    TakeResult takePrefixFromTransactionId(Consistency consistency,
        byte[] prefix,
        long txId,
        Highwaters highwaters,
        Scan scan) throws Exception;

    // TODO fix or deprecate: Currently know to be broken. Only accurate if you never delete.
    long count() throws Exception;

    void highestTxId(HighestPartitionTx highestPartitionTx) throws Exception;

}
