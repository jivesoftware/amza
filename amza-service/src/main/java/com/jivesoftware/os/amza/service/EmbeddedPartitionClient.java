package com.jivesoftware.os.amza.service;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.PrefixedKeyRanges;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream.TxResult;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.service.Partition.ScanRange;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class EmbeddedPartitionClient implements PartitionClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Partition partition;
    private final RingMember rootRingMember;

    public EmbeddedPartitionClient(Partition partition, RingMember rootRingMember) {
        this.partition = partition;
        this.rootRingMember = rootRingMember;
    }

    @Override
    public void commit(Consistency consistency,
        byte[] prefix,
        ClientUpdates updates,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        partition.commit(consistency, prefix, updates, abandonSolutionAfterNMillis);
    }

    @Override
    public long getApproximateCount(Consistency consistency, long additionalSolverAfterNMillis, long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis, Optional<List<String>> solutionLog) throws Exception {
        return partition.approximateCount();
    }

    @Override
    public boolean get(Consistency consistency,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueTimestampStream valuesStream,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        return partition.get(consistency, prefix, true, keys, (prefix1, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
            if (valueTimestamp == -1 || valueTombstoned) {
                return valuesStream.stream(prefix1, key, null, -1, -1);
            } else {
                return valuesStream.stream(prefix1, key, value, valueTimestamp, valueVersion);
            }
        });
    }

    @Override
    public boolean getRaw(Consistency consistency,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueStream valuesStream,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        return partition.get(consistency, prefix, true, keys, valuesStream);
    }

    @Override
    public boolean scan(Consistency consistency, boolean compressed, PrefixedKeyRanges ranges, KeyValueTimestampStream scan, long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis, long abandonSolutionAfterNMillis, Optional<List<String>> solutionLog) throws Exception {
        return scanInternal(consistency, compressed, ranges, scan, true, additionalSolverAfterNMillis, abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            solutionLog);
    }

    @Override
    public boolean scanKeys(Consistency consistency, boolean compressed, PrefixedKeyRanges ranges, KeyValueTimestampStream scan,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis, long abandonSolutionAfterNMillis, Optional<List<String>> solutionLog) throws Exception {
        return scanInternal(consistency, compressed, ranges, scan, false, additionalSolverAfterNMillis, abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            solutionLog);
    }

    private boolean scanInternal(Consistency consistency,
        boolean compressed,
        PrefixedKeyRanges ranges,
        KeyValueTimestampStream stream,
        boolean hydrateValues,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {

        List<ScanRange> scanRanges = Lists.newArrayList();
        ranges.consume((fromPrefix, fromKey, toPrefix, toKey) -> {
            scanRanges.add(new ScanRange(fromPrefix, fromKey, toPrefix, toKey));
            return true;
        });
        return partition.scan(scanRanges, hydrateValues, true,
            (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                return valueTombstoned || stream.stream(prefix, key, value, valueTimestamp, valueVersion);
            });
    }

    @Override
    public TakeResult takeFromTransactionId(List<RingMember> membersInOrder,
        Map<RingMember, Long> memberTxIds,
        int limit,
        Highwaters highwaters,
        TxKeyValueStream stream,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        if (!membersInOrder.contains(rootRingMember)) {
            LOG.warn("Took from {} but not in desired members {}", rootRingMember, membersInOrder);
            return new TakeResult(rootRingMember, -1L, null);
        }
        long txId = memberTxIds.getOrDefault(rootRingMember, -1L);
        int[] count = { 0 };
        return partition.takeFromTransactionId(txId, true, highwaters,
            (rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                TxResult result = stream.stream(rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
                count[0]++;
                return (limit > 0 && count[0] >= limit) ? TxResult.ACCEPT_AND_STOP : result;
            });
    }

    @Override
    public TakeResult takePrefixFromTransactionId(List<RingMember> membersInOrder,
        byte[] prefix,
        Map<RingMember, Long> memberTxIds,
        int limit,
        Highwaters highwaters,
        TxKeyValueStream stream,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        if (!membersInOrder.contains(rootRingMember)) {
            LOG.warn("Took with prefix from {} but not in desired members {}", rootRingMember, membersInOrder);
            return new TakeResult(rootRingMember, -1L, null);
        }
        long txId = memberTxIds.getOrDefault(rootRingMember, -1L);
        int[] count = { 0 };
        return partition.takePrefixFromTransactionId(prefix, txId, true, highwaters,
            (rowTxId, prefix1, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                TxResult result = stream.stream(rowTxId, prefix1, key, value, valueTimestamp, valueTombstoned, valueVersion);
                count[0]++;
                return (limit > 0 && count[0] >= limit) ? TxResult.ACCEPT_AND_STOP : result;
            });
    }

}
