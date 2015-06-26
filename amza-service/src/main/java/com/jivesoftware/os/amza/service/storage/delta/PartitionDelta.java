package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.delta.DeltaWAL.KeyValueHighwater;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
import com.jivesoftware.os.amza.shared.wal.WALTimestampId;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.storage.WALRow;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
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

/**
 * @author jonathan.colt
 */
class PartitionDelta {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final VersionedPartitionName versionedPartitionName;
    private final DeltaWAL deltaWAL;
    private final WALUpdated walUpdated;
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    final AtomicReference<PartitionDelta> compacting;

    private final Map<WALKey, WALPointer> pointerIndex = new ConcurrentHashMap<>();
    private final ConcurrentNavigableMap<WALKey, WALPointer> orderedIndex = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Long, List<Long>> txIdWAL = new ConcurrentSkipListMap<>();
    private final AtomicLong updatesSinceLastHighwaterFlush = new AtomicLong();

    PartitionDelta(VersionedPartitionName versionedPartitionName,
        DeltaWAL deltaWAL,
        WALUpdated walUpdated,
        PrimaryRowMarshaller<byte[]> primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        PartitionDelta compacting) {
        this.versionedPartitionName = versionedPartitionName;
        this.deltaWAL = deltaWAL;
        this.walUpdated = walUpdated;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
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
                missed |= (got == null);
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

    void put(WALKey key, WALPointer rowPointer) {
        pointerIndex.put(key, rowPointer);
        orderedIndex.put(key, rowPointer);
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

    void appendTxFps(long rowTxId, long rowFP) {
        List<Long> fps = txIdWAL.get(rowTxId);
        if (fps == null) {
            fps = new ArrayList<>();
            txIdWAL.put(rowTxId, fps);
        }
        fps.add(rowFP);
    }

    long highestTxId() {
        if (txIdWAL.isEmpty()) {
            return -1;
        }
        return txIdWAL.lastKey();
    }

    void appendTxFps(long rowTxId, Collection<Long> rowFPs) {
        List<Long> fps = txIdWAL.get(rowTxId);
        if (fps != null) {
            throw new IllegalStateException("Already appended this txId: " + rowTxId);
        }
        txIdWAL.put(rowTxId, ImmutableList.copyOf(rowFPs));
        updatesSinceLastHighwaterFlush.addAndGet(rowFPs.size());
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

        ConcurrentNavigableMap<Long, List<Long>> tailMap = txIdWAL.tailMap(transactionId, false);
        return deltaWAL.takeRows(tailMap, rowStream);
    }

    public boolean takeFromTransactionId(final long transactionId, Highwaters highwaters, final Scan<WALValue> scan) throws Exception {
        PartitionDelta partitionDelta = compacting.get();
        if (partitionDelta != null) {
            if (!partitionDelta.takeFromTransactionId(transactionId, highwaters, scan)) {
                return false;
            }
        }

        if (txIdWAL.isEmpty() || txIdWAL.lastEntry().getKey() < transactionId) {
            return true;
        }

        ConcurrentNavigableMap<Long, List<Long>> tailMap = txIdWAL.tailMap(transactionId, false);
        return deltaWAL.takeRows(tailMap, (long rowFP, long rowTxId, RowType rowType, byte[] row) -> {
            if (rowType == RowType.primary) {
                WALRow walRow = primaryRowMarshaller.fromRow(row);
                return scan.row(rowTxId, walRow.key, walRow.value);
            } else if (rowType == RowType.highwater) {
                highwaters.highwater(highwaterRowMarshaller.fromBytes(row));
            }
            return true;
        });
    }

    void compact(PartitionIndex partitionIndex) throws Exception {
        final PartitionDelta compact = compacting.get();
        if (compact != null) {
            if (!compact.txIdWAL.isEmpty()) {
                final PartitionStore partitionStore = partitionIndex.get(compact.versionedPartitionName);
                final long highestTxId = partitionStore.highestTxId();
                LOG.info("Merging ({}) deltas for partition: {} from tx: {}", compact.orderedIndex.size(), compact.versionedPartitionName, highestTxId);
                LOG.debug("Merging keys: {}", compact.orderedIndex.keySet());
                partitionStore.directCommit(true, Status.COMPACTING, (highwater, scan) -> {
                    try {
                        eos:
                        for (Map.Entry<Long, List<Long>> e : compact.txIdWAL.tailMap(highestTxId, true).entrySet()) {
                            long txId = e.getKey();
                            for (long fp : e.getValue()) {
                                KeyValueHighwater kvh = compact.deltaWAL.hydrateKeyValueHighwater(fp);

                                ByteBuffer bb = ByteBuffer.wrap(kvh.key.getKey());
                                byte[] partitionNameBytes = new byte[bb.getShort()];
                                bb.get(partitionNameBytes);
                                final byte[] keyBytes = new byte[bb.getInt()];
                                bb.get(keyBytes);

                                WALKey key = new WALKey(keyBytes);
                                WALPointer pointer = compact.orderedIndex.get(key);
                                if (pointer == null) {
                                    throw new RuntimeException("Delta WAL missing key: " + key);
                                }
                                if (pointer.getFp() == fp) {
                                    if (!scan.row(txId, key, kvh.value)) {
                                        break eos;
                                    }
                                    if (kvh.highwater != null) {
                                        highwater.highwater(kvh.highwater);
                                    }
                                }
                            }
                        }
                    } catch (Throwable ex) {
                        throw new RuntimeException("Error while streaming entry set.", ex);
                    }
                }, walUpdated);
                LOG.info("Merged deltas for " + compact.versionedPartitionName);
            }
        }
        compacting.set(null);
    }

}
