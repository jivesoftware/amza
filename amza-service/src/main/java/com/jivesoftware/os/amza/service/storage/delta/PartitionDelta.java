package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterators;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.delta.DeltaValueCache.DeltaRow;
import com.jivesoftware.os.amza.shared.StripedTLongObjectMap;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.wal.FpKeyValueHighwaterStream;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
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

    private boolean getRawValue(byte[] key, FpKeyValueHighwaterStream stream) throws Exception {
        WALPointer got = pointerIndex.get(new WALKey(key));
        if (got == null) {
            PartitionDelta partitionDelta = compacting.get();
            if (partitionDelta != null) {
                return partitionDelta.getRawValue(key, stream);
            }
            return stream.stream(-1, key, null, -1, false, null);
        }
        return deltaWAL.hydrate(got.getFp(), stream);
    }

    boolean get(WALKeys keys, KeyValueStream stream) throws Exception {
        return keys.consume((key) ->
            getRawValue(key, (fp, key1, value1, valueTimestamp1, valueTombstone1, highwater) ->
                stream.stream(key, value1, valueTimestamp1, valueTombstone1)));
    }

    WALPointer getPointer(byte[] key) throws Exception {
        WALPointer got = pointerIndex.get(new WALKey(key));
        if (got != null) {
            return got;
        }
        PartitionDelta partitionDelta = compacting.get();
        if (partitionDelta != null) {
            return partitionDelta.getPointer(key);
        }
        return null;
    }

    boolean getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        return keyValues.consume((key, value, valueTimestamp, valueTombstone) -> {
            WALPointer pointer = getPointer(key);
            if (pointer != null) {
                return stream.stream(key, value, valueTimestamp, valueTombstone, pointer.getTimestampId(), pointer.getTombstoned(), pointer.getFp());
            } else {
                return stream.stream(key, value, valueTimestamp, valueTombstone, -1, false, -1);
            }
        });
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

    void put(long fp,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstone,
        WALHighwater highwater) {

        WALPointer pointer = new WALPointer(fp, valueTimestamp, valueTombstone);
        WALKey walKey = new WALKey(key);
        pointerIndex.put(walKey, pointer);
        orderedIndex.put(walKey, pointer);
        deltaValueCache.put(fp, key, value, valueTimestamp, valueTombstone, highwater, deltaValueMap);
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
            fps = new long[]{rowFP};
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

    boolean takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
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
                    PartitionStore partitionStore = partitionIndex.get(compact.versionedPartitionName);
                    long highestTxId = partitionStore.highestTxId();
                    LOG.info("Merging ({}) deltas for partition: {} from tx: {}", compact.orderedIndex.size(), compact.versionedPartitionName, highestTxId);
                    LOG.debug("Merging keys: {}", compact.orderedIndex.keySet());
                    MutableBoolean eos = new MutableBoolean(false);
                    for (Map.Entry<Long, long[]> e : compact.txIdWAL.tailMap(highestTxId, true).entrySet()) {
                        long txId = e.getKey();
                        partitionStore.directCommit(true, (highwaters, scan) -> {
                            for (long fp : e.getValue()) {
                                boolean done = compact.valueForFp(fp,
                                    (fp1, key, value, valueTimestamp, valueTombstone, walHighwater) -> {
                                        WALKey walKey = new WALKey(key);
                                        WALPointer pointer = compact.orderedIndex.get(walKey);
                                        if (pointer == null) {
                                            throw new RuntimeException("Delta WAL missing key: " + walKey);
                                        }
                                        if (pointer.getFp() == fp) {
                                            if (!scan.row(txId, key, value, valueTimestamp, valueTombstone)) {
                                                eos.setValue(true);
                                                return false;
                                            }
                                            if (walHighwater != null) {
                                                highwaters.highwater(walHighwater);
                                            }
                                        }
                                        return true;
                                    });
                                if (!done) {
                                    return false;
                                }
                            }
                            return true;
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

    private boolean valueForFp(long fp, FpKeyValueHighwaterStream stream) throws Exception {
        DeltaRow deltaRow = deltaValueCache.get(fp, deltaValueMap);
        if (deltaRow != null) {
            return stream.stream(deltaRow.fp, deltaRow.key, deltaRow.value, deltaRow.valueTimestamp, deltaRow.valueTombstone, deltaRow.highwater);
        } else {
            return deltaWAL.hydrateKeyValueHighwater(fp, stream);
        }
    }
}
