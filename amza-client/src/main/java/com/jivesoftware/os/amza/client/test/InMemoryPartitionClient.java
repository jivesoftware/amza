package com.jivesoftware.os.amza.client.test;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.PrefixedKeyRanges;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Allows you to write tests without standing up an amza cluster.
 */
public class InMemoryPartitionClient implements PartitionClient {

    private final ConcurrentSkipListMap<byte[], WALValue> index;
    private final OrderIdProvider orderIdProvider;

    public InMemoryPartitionClient(ConcurrentSkipListMap<byte[], WALValue> index, OrderIdProvider orderIdProvider) {
        this.index = index;
        this.orderIdProvider = orderIdProvider;
    }

    @Override
    public void commit(Consistency consistency,
        byte[] prefix,
        ClientUpdates updates,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {

        updates.updates((key, value, valueTimestamp, valueTombstoned) -> {
            long version = orderIdProvider.nextId();
            if (valueTimestamp == -1) {
                valueTimestamp = version;
            }
            WALValue update = new WALValue(RowType.primary, value, valueTimestamp, valueTombstoned, version);
            index.compute(WALKey.compose(null, key), (key1, existing) -> {
                if (existing != null
                    && (existing.getTimestampId() > update.getTimestampId()
                    || (existing.getTimestampId() == update.getTimestampId() && existing.getVersion() > update.getVersion()))) {
                    return existing;
                } else {
                    return update;
                }
            });
            return true;
        });
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

        return keys.consume(key -> {
            WALValue walValue = index.get(WALKey.compose(prefix, key));
            if (walValue != null && walValue.getTimestampId() != -1 && !walValue.getTombstoned()) {
                return valuesStream.stream(prefix, key, walValue.getValue(), walValue.getTimestampId(), walValue.getVersion());
            } else {
                return valuesStream.stream(prefix, key, null, -1, -1);
            }
        });
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
        KeyValueTimestampStream scan,
        boolean hydrateValues,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {

        return ranges.consume((fromPrefix, fromKey, toPrefix, toKey) -> {
            Set<Map.Entry<byte[], WALValue>> entries;
            if (fromKey == null && toKey == null) {
                entries = index.entrySet();
            } else if (fromKey == null) {
                entries = index.headMap(WALKey.compose(toPrefix, toKey), false).entrySet();
            } else if (toKey == null) {
                entries = index.tailMap(WALKey.compose(fromPrefix, fromKey), true).entrySet();
            } else {
                entries = index.subMap(WALKey.compose(fromPrefix, fromKey), true, WALKey.compose(toPrefix, toKey), false).entrySet();
            }
            for (Map.Entry<byte[], WALValue> entry : entries) {
                byte[] rawKey = entry.getKey();
                byte[] prefix = WALKey.rawKeyPrefix(rawKey);
                byte[] key = WALKey.rawKeyKey(rawKey);
                WALValue value = entry.getValue();
                if (value.getTimestampId() != -1 && !value.getTombstoned()) {
                    if (!scan.stream(prefix, key, hydrateValues ? value.getValue() : null, value.getTimestampId(), value.getVersion())) {
                        return false;
                    }
                }
            }
            return true;
        });
    }

    @Override
    public TakeResult takeFromTransactionId(List<RingMember> membersInOrder,
        Map<RingMember, Long> memberTxIds,
        Highwaters highwaters,
        TxKeyValueStream stream,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        throw new UnsupportedOperationException("Not yet");
    }

    @Override
    public TakeResult takePrefixFromTransactionId(List<RingMember> membersInOrder,
        byte[] prefix,
        Map<RingMember, Long> memberTxIds,
        Highwaters highwaters,
        TxKeyValueStream stream,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        throw new UnsupportedOperationException("Not yet");
    }

    @Override
    public long getApproximateCount(Consistency consistency,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        return index.size();
    }

}
