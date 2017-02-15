package com.jivesoftware.os.amza.client.test;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.OffsetUnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.PrefixedKeyRanges;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream.TxResult;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Allows you to write tests without standing up an amza cluster.
 */
public class InMemoryPartitionClient implements PartitionClient {

    private final RingMember ringMember;
    private final ConcurrentSkipListMap<Long, Tx> transactions;
    private final ConcurrentSkipListMap<byte[], WALValue> index;
    private final OrderIdProvider orderIdProvider;

    private final AtomicLong txProvider = new AtomicLong();

    public InMemoryPartitionClient(RingMember ringMember,
        ConcurrentSkipListMap<Long, Tx> transactions,
        ConcurrentSkipListMap<byte[], WALValue> index,
        OrderIdProvider orderIdProvider) {
        this.ringMember = ringMember;
        this.transactions = transactions;
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
                    long txId = txProvider.incrementAndGet();
                    transactions.put(txId, new Tx(txId, prefix, key, update.getValue(), update.getTimestampId(), update.getTombstoned()));
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
    public boolean getOffset(Consistency consistency,
        byte[] prefix,
        OffsetUnprefixedWALKeys keys,
        KeyValueTimestampStream valuesStream,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {

        return keys.consume((key, offset, length) -> {
            WALValue walValue = index.get(WALKey.compose(prefix, key));
            if (walValue != null && walValue.getTimestampId() != -1 && !walValue.getTombstoned()) {
                byte[] value = walValue.getValue();
                if (offset == 0 && length >= value.length) {
                    // do nothing
                } else if (offset >= value.length) {
                    value = null;
                } else {
                    int available = Math.min(length, value.length - offset);
                    byte[] sub = new byte[available];
                    System.arraycopy(value, offset, sub, 0, available);
                    value = sub;
                }
                return valuesStream.stream(prefix, key, value, walValue.getTimestampId(), walValue.getVersion());
            } else {
                return valuesStream.stream(prefix, key, null, -1, -1);
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

        return keys.consume(key -> {
            WALValue walValue = index.get(WALKey.compose(prefix, key));
            if (walValue != null) {
                return valuesStream.stream(prefix, key, walValue.getValue(), walValue.getTimestampId(), walValue.getTombstoned(), walValue.getVersion());
            } else {
                return valuesStream.stream(prefix, key, null, -1, false, -1);
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
        int limit,
        Highwaters highwaters,
        TxKeyValueStream stream,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        Long fromTxId = memberTxIds != null ? memberTxIds.getOrDefault(ringMember, 0L) : 0L;
        ConcurrentNavigableMap<Long, Tx> take = transactions.tailMap(fromTxId, false);
        long lastTxId = -1;
        boolean tookToEnd = true;
        for (Tx tx : take.values()) {
            TxResult result = stream.stream(tx.txId, tx.prefix, tx.key, tx.value, tx.valueTimestamp, tx.valueTombstoned, 0L);
            if (result.isAccepted()) {
                lastTxId = tx.txId;
            }
            if (!result.wantsMore()) {
                tookToEnd = false;
                break;
            }
        }

        return new TakeResult(ringMember,
            lastTxId,
            tookToEnd ? new WALHighwater(Collections.singletonList(new RingMemberHighwater(ringMember, transactions.lastKey()))) : null);
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

    public static class Tx {

        public final long txId;
        public final byte[] prefix;
        public final byte[] key;
        public final byte[] value;
        public final long valueTimestamp;
        public final boolean valueTombstoned;

        public Tx(long txId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) {
            this.txId = txId;
            this.prefix = prefix;
            this.key = key;
            this.value = value;
            this.valueTimestamp = valueTimestamp;
            this.valueTombstoned = valueTombstoned;
        }
    }
}
