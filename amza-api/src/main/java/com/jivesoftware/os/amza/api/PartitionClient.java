package com.jivesoftware.os.amza.api;

import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.OffsetUnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.PrefixedKeyRanges;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception;

    long getApproximateCount(Consistency consistency,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception;

    boolean get(Consistency consistency,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueTimestampStream valuesStream,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception;

    boolean getOffset(Consistency consistency,
        byte[] prefix,
        OffsetUnprefixedWALKeys keys,
        KeyValueTimestampStream valuesStream,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception;

    boolean getRaw(Consistency consistency,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueStream valuesStream,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception;

    /**
     * @param ranges the ranges (from key/prefix is nullable and inclusive, to key/prefix is nullable and exclusive)
     */
    boolean scan(Consistency consistency,
        boolean compressed,
        PrefixedKeyRanges ranges,
        KeyValueTimestampStream scan,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception;

    interface KeyValueFilter extends Serializable {
        boolean filter(byte[] prefix, byte[] key, byte[] value, long timestamp, boolean tombstoned, long version, KeyValueStream stream) throws Exception;
    }

    boolean scanFiltered(Consistency consistency,
        boolean compressed,
        PrefixedKeyRanges ranges,
        KeyValueFilter filter,
        KeyValueTimestampStream scan,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception;

    boolean scanKeys(Consistency consistency,
        boolean compressed,
        PrefixedKeyRanges ranges,
        KeyValueTimestampStream scan,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception;

    TakeResult takeFromTransactionId(List<RingMember> membersInOrder,
        Map<RingMember, Long> memberTxIds,
        int limit,
        Highwaters highwaters,
        TxKeyValueStream stream,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws
        Exception;

    TakeResult takePrefixFromTransactionId(List<RingMember> membersInOrder,
        byte[] prefix,
        Map<RingMember, Long> memberTxIds,
        int limit,
        Highwaters highwaters,
        TxKeyValueStream stream,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception;

}
