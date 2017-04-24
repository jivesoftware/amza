package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.PartitionClient.KeyValueFilter;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.OffsetUnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.PrefixedKeyRanges;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public interface RemotePartitionCaller<C, E extends Throwable> {

    PartitionResponse<NoOpCloseable> commit(RingMember leader,
        RingMember ringMember,
        C client,
        Consistency consistency,
        byte[] prefix,
        ClientUpdates updates,
        long abandonSolutionAfterNMillis) throws E;

    PartitionResponse<CloseableStreamResponse> get(RingMember leader,
        RingMember ringMember,
        C client,
        Consistency consistency,
        byte[] prefix,
        UnprefixedWALKeys keys) throws E;

    PartitionResponse<CloseableStreamResponse> getOffset(RingMember leader,
        RingMember ringMember,
        C client,
        Consistency consistency,
        byte[] prefix,
        OffsetUnprefixedWALKeys keys) throws E;

    PartitionResponse<CloseableStreamResponse> scan(RingMember leader,
        RingMember ringMember,
        C client,
        Consistency consistency,
        boolean compressed,
        PrefixedKeyRanges ranges,
        KeyValueFilter filter,
        boolean hydrateValues) throws E;

    PartitionResponse<CloseableStreamResponse> takeFromTransactionId(RingMember leader,
        RingMember ringMember,
        C client,
        Map<RingMember, Long> membersTxId,
        int limit) throws E;

    PartitionResponse<CloseableStreamResponse> takePrefixFromTransactionId(RingMember leader,
        RingMember ringMember,
        C client,
        byte[] prefix,
        Map<RingMember, Long> membersTxId,
        int limit) throws E;

    PartitionResponse<CloseableLong> getApproximateCount(RingMember leader,
        RingMember ringMember,
        C client) throws E;
}
