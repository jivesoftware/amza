package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.take.TakeResult;
import com.jivesoftware.os.amza.shared.wal.TimestampKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;

/**
 * @author jonathan.colt
 */
public interface AmzaPartitionAPI {

    void commit(Commitable updates,
        int desiredQuorum,
        long timeoutInMillis) throws Exception;

    boolean get(WALKeys keys, TimestampKeyValueStream valuesStream) throws Exception;

    /**
     * @param from   nullable (inclusive)
     * @param to     nullable (exclusive)
     * @param stream
     * @throws Exception
     */
    void scan(byte[] from, byte[] to, Scan<TimestampedValue> stream) throws Exception;

    TakeResult takeFromTransactionId(long transactionId, Highwaters highwaters, Scan<TimestampedValue> scan) throws Exception;

    long count() throws Exception;

    void highestTxId(HighestPartitionTx highestPartitionTx) throws Exception;

}
