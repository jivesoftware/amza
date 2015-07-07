package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.delta.DeltaValueCache.DeltaRow;
import com.jivesoftware.os.amza.service.storage.delta.DeltaWAL.KeyValueHighwater;
import com.jivesoftware.os.amza.shared.StripedTLongObjectMap;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
import com.jivesoftware.os.amza.shared.wal.WALTimestampId;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableBoolean;

/**
 * @author jonathan.colt
 */
class PartitionDelta {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final VersionedPartitionName versionedPartitionName;
    private final DeltaWAL deltaWAL;
    private final DeltaValueCache deltaValueCache;
    final AtomicReference<PartitionDelta> compacting;

    private final Map<WALKey, WALPointer> pointerIndex = new ConcurrentHashMap<>();
    private final ConcurrentNavigableMap<WALKey, WALPointer> orderedIndex = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Long, long[]> txIdWAL = new ConcurrentSkipListMap<>();
    private final StripedTLongObjectMap<DeltaRow> deltaValueMap = new StripedTLongObjectMap<>(8);
    private final AtomicLong updatesSinceLastHighwaterFlush = new AtomicLong();

    PartitionDelta(VersionedPartitionName versionedPartitionName,
        DeltaWAL deltaWAL,
        DeltaValueCache deltaValueCache,
        PartitionDelta compacting) {
        this.versionedPartitionName = versionedPartitionName;
        this.deltaWAL = deltaWAL;
        this.deltaValueCache = deltaValueCache;
        this.compacting = new AtomicReference<>(compacting);
    }

    Optional<WALValue> get(WALKey key) throws Exception {
        WALPointer got = pointerIndex.get(key);
        if (got == null) {
            PartitionDelta partitionDelta = compacting.get();
            if (partitionDelta != null) {
                return partitionDelta.get(key);
            }
            return null;
        }
        return Optional.fromNullable(got.getTombstoned() ? null : deltaWAL.hydrate(got.getFp()).value);
    }

    WALTimestampId getTimestampId(WALKey key) throws Exception {
        WALPointer got = pointerIndex.get(key);
        if (got != null) {
            return new WALTimestampId(got.getTimestampId(), got.getTombstoned());
        }
        PartitionDelta partitionDelta = compacting.get();
        if (partitionDelta != null) {
            return partitionDelta.getTimestampId(key);
        }
        return null;
    }

    DeltaResult<WALTimestampId[]> getTimestampIds(WALKey[] consumableKeys) throws Exception {
        boolean missed = false;
        WALTimestampId[] result = new WALTimestampId[consumableKeys.length];
        for (int i = 0; i < consumableKeys.length; i++) {
            WALKey key = consumableKeys[i];
            if (key != null) {
                WALTimestampId got = getTimestampId(key);
                if (got != null) {
                    result[i] = got;
                    consumableKeys[i] = null;
                } else {
                    missed = true;
                }
            }
        }
        return new DeltaResult<>(missed, result);
    }

    DeltaResult<List<WALValue>> get(List<WALKey> keys) throws Exception {
        boolean missed = false;
        List<WALValue> result = new ArrayList<>(keys.size());
        for (WALKey key : keys) {
            Optional<WALValue> got = get(key);
            if (got == null) {
                missed |= true;
                result.add(null);
            } else {
                result.add(got.orNull());
            }
        }
        return new DeltaResult<>(missed, result);
    }

    Boolean containsKey(WALKey key) {
        WALPointer got = pointerIndex.get(key);
        if (got != null) {
            return !got.getTombstoned();
        }
        PartitionDelta partitionDelta = compacting.get();
        if (partitionDelta != null) {
            return partitionDelta.containsKey(key);
        }
        return null;
    }

    DeltaResult<List<Boolean>> containsKey(List<WALKey> keys) {
        boolean missed = false;
        List<Boolean> result = new ArrayList<>(keys.size());
        for (WALKey key : keys) {
            boolean got = containsKey(key);
            missed |= got;
            result.add(got);
        }
        return new DeltaResult<>(missed, result);
    }

    void put(long fp, KeyValueHighwater keyValueHighwater) {
        WALKey key = keyValueHighwater.key;
        WALValue value = keyValueHighwater.value;
        WALPointer pointer = new WALPointer(fp, value.getTimestampId(), value.getTombstoned());
        pointerIndex.put(key, pointer);
        orderedIndex.put(key, pointer);
        deltaValueCache.put(fp, keyValueHighwater, deltaValueMap);
    }

    boolean shouldWriteHighwater() {
        long got = updatesSinceLastHighwaterFlush.get();
        boolean should = false;
        if (got == 0) {
            should = true;
        }
        if (got > 1000) { // TODO expose to partition config
            updatesSinceLastHighwaterFlush.set(0);
        }
        return should;
    }

    Set<WALKey> keySet() {
        Set<WALKey> keySet = pointerIndex.keySet();
        PartitionDelta partitionDelta = compacting.get();
        if (partitionDelta != null) {
            HashSet<WALKey> all = new HashSet<>(keySet);
            all.addAll(partitionDelta.keySet());
            return all;
        }
        return keySet;
    }

    DeltaPeekableElmoIterator rangeScanIterator(WALKey from, WALKey to) {
        Iterator<Map.Entry<WALKey, WALPointer>> iterator = orderedIndex.subMap(from, to).entrySet().iterator();
        Iterator<Map.Entry<WALKey, WALPointer>> compactingIterator = Iterators.emptyIterator();
        PartitionDelta compactingPartitionDelta = compacting.get();
        DeltaWAL compactingDeltaWAL = null;
        if (compactingPartitionDelta != null) {
            compactingIterator = compactingPartitionDelta.orderedIndex.subMap(from, to).entrySet().iterator();
            compactingDeltaWAL = compactingPartitionDelta.deltaWAL;
        }
        return new DeltaPeekableElmoIterator(iterator, compactingIterator, deltaWAL, compactingDeltaWAL);
    }

    DeltaPeekableElmoIterator rowScanIterator() {
        Iterator<Map.Entry<WALKey, WALPointer>> iterator = orderedIndex.entrySet().iterator();
        Iterator<Map.Entry<WALKey, WALPointer>> compactingIterator = Iterators.emptyIterator();
        PartitionDelta compactingPartitionDelta = compacting.get();
        DeltaWAL compactingDeltaWAL = null;
        if (compactingPartitionDelta != null) {
            compactingIterator = compactingPartitionDelta.orderedIndex.entrySet().iterator();
            compactingDeltaWAL = compactingPartitionDelta.deltaWAL;
        }
        return new DeltaPeekableElmoIterator(iterator, compactingIterator, deltaWAL, compactingDeltaWAL);
    }

    long highestTxId() {
        if (txIdWAL.isEmpty()) {
            return -1;
        }
        return txIdWAL.lastKey();
    }

    public long lowestTxId() {
        PartitionDelta partitionDelta = compacting.get();
        if (partitionDelta != null) {
            long lowestTxId = partitionDelta.lowestTxId();
            if (lowestTxId >= 0) {
                return lowestTxId;
            }
        }

        if (txIdWAL.isEmpty()) {
            return -1;
        }
        return txIdWAL.firstKey();
    }

    void appendTxFps(long rowTxId, long rowFP) {
        long[] fps = txIdWAL.get(rowTxId);
        if (fps == null) {
            fps = new long[] { rowFP };
            txIdWAL.put(rowTxId, fps);
        } else {
            long[] swap = new long[fps.length + 1];
            System.arraycopy(fps, 0, swap, 0, fps.length);
            swap[swap.length - 1] = rowFP;
        }
    }

    void appendTxFps(long rowTxId, long[] rowFPs) {
        long[] existing = txIdWAL.putIfAbsent(rowTxId, rowFPs);
        if (existing != null) {
            throw new IllegalStateException("Already appended this txId: " + rowTxId);
        }
        updatesSinceLastHighwaterFlush.addAndGet(rowFPs.length);
    }

    boolean takeRowUpdatesSince(long transactionId, final RowStream rowStream) throws Exception {
        PartitionDelta partitionDelta = compacting.get();
        if (partitionDelta != null) {
            if (!partitionDelta.takeRowUpdatesSince(transactionId, rowStream)) {
                return false;
            }
        }

        if (txIdWAL.isEmpty() || txIdWAL.lastEntry().getKey() < transactionId) {
            return true;
        }

        ConcurrentNavigableMap<Long, long[]> tailMap = txIdWAL.tailMap(transactionId, false);
        return deltaWAL.takeRows(tailMap, rowStream, deltaValueCache, deltaValueMap);
    }

    public boolean takeRowsFromTransactionId(final long transactionId, RowStream rowStream) throws Exception {
        PartitionDelta partitionDelta = compacting.get();
        if (partitionDelta != null) {
            if (!partitionDelta.takeRowsFromTransactionId(transactionId, rowStream)) {
                return false;
            }
        }

        if (txIdWAL.isEmpty() || txIdWAL.lastEntry().getKey() < transactionId) {
            return true;
        }

        ConcurrentNavigableMap<Long, long[]> tailMap = txIdWAL.tailMap(transactionId, false);
        return deltaWAL.takeRows(tailMap, rowStream, deltaValueCache, deltaValueMap);
    }

    void compact(PartitionIndex partitionIndex) throws Exception {
        final PartitionDelta compact = compacting.get();
        if (compact != null) {
            if (!compact.txIdWAL.isEmpty()) {
                try {
                    final PartitionStore partitionStore = partitionIndex.get(compact.versionedPartitionName);
                    final long highestTxId = partitionStore.highestTxId();
                    LOG.info("Merging ({}) deltas for partition: {} from tx: {}", compact.orderedIndex.size(), compact.versionedPartitionName, highestTxId);
                    LOG.debug("Merging keys: {}", compact.orderedIndex.keySet());
                    MutableBoolean eos = new MutableBoolean(false);
                    for (Map.Entry<Long, long[]> e : compact.txIdWAL.tailMap(highestTxId, true).entrySet()) {
                        long txId = e.getKey();
                        partitionStore.directCommit(true, (highwater, scan) -> {
                            for (long fp : e.getValue()) {
                                KeyValueHighwater kvh = compact.valueForFp(fp);
                                WALPointer pointer = compact.orderedIndex.get(kvh.key);
                                if (pointer == null) {
                                    throw new RuntimeException("Delta WAL missing key: " + kvh.key);
                                }
                                if (pointer.getFp() == fp) {
                                    if (!scan.row(txId, kvh.key, kvh.value)) {
                                        eos.setValue(true);
                                        return;
                                    }
                                    if (kvh.highwater != null) {
                                        highwater.highwater(kvh.highwater);
                                    }
                                }
                            }
                        });
                        if (eos.booleanValue()) {
                            break;
                        }
                    }
                    LOG.info("Merged deltas for {}", compact.versionedPartitionName);
                } catch (Throwable ex) {
                    throw new RuntimeException("Error while streaming entry set.", ex);
                }
            }
        }
        compacting.set(null);
    }

    private KeyValueHighwater valueForFp(long fp) throws Exception {
        DeltaRow deltaRow = deltaValueCache.get(fp, deltaValueMap);
        if (deltaRow != null) {
            return deltaRow.keyValueHighwater;
        } else {
            return deltaWAL.hydrateKeyValueHighwater(fp);
        }
    }
}
