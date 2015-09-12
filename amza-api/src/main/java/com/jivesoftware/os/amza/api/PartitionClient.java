package com.jivesoftware.os.amza.api;

import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public interface PartitionClient {

    /*
    // TODO impl a blob commit and get.
    // Blobs are chunked as rows and are typically larger than anything you would like to keep in ram.
    void commitBlob(Consistency consistency, byte[] prefix,
        byte[] key, InputStream value, long valueTimestamp,  boolean valueTombstoned, long valueVersion,
        long timeoutInMillis) throws Exception;

     boolean getBlob(Consistency consistency,
        byte[] prefix,
        byte[] keys,
        ValueStream valueStream) throws Exception;
    */

    void commit(Consistency consistency, byte[] prefix,
        ClientUpdates updates,
        long timeoutInMillis) throws Exception;

    boolean get(Consistency consistency,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueTimestampStream valuesStream) throws Exception;

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

    TakeResult takeFromTransactionId(List<RingMember> membersInOrder,
        Map<RingMember, Long> memberTxIds,
        Highwaters highwaters,
        TxKeyValueStream stream) throws
        Exception;

    TakeResult takePrefixFromTransactionId(List<RingMember> membersInOrder,
        byte[] prefix,
        Map<RingMember, Long> memberTxIds,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception;

}
