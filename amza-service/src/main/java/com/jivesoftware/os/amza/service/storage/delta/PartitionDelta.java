package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterators;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.wal.FpKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.KeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeyStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
    final AtomicReference<PartitionDelta> merging;

    private final Map<WALKey, WALPointer> pointerIndex = new ConcurrentHashMap<>(); //TODO replace with concurrent byte[] map
    private final ConcurrentSkipListMap<byte[], WALPointer> orderedIndex = new ConcurrentSkipListMap<>(WALKey::compare);
    private final ConcurrentSkipListMap<Long, long[]> txIdWAL = new ConcurrentSkipListMap<>();
    private final AtomicLong updatesSinceLastHighwaterFlush = new AtomicLong();

    PartitionDelta(VersionedPartitionName versionedPartitionName,
        DeltaWAL deltaWAL,
        PartitionDelta merging) {
        this.versionedPartitionName = versionedPartitionName;
        this.deltaWAL = deltaWAL;
        this.merging = new AtomicReference<>(merging);
    }

    public long size() {
        return pointerIndex.size();
    }

    private boolean streamRawValues(WALKeys keys, FpKeyValueStream fpKeyValueStream) throws Exception {
        return deltaWAL.hydrate(fpStream -> {
            PartitionDelta mergingPartitionDelta = merging.get();
            if (mergingPartitionDelta != null) {
                return mergingPartitionDelta.streamRawValues(
                    mergingKeyStream -> keys.consume((prefix, key) -> {
                        WALPointer got = pointerIndex.get(new WALKey(prefix, key));
                        if (got == null) {
                            return mergingKeyStream.stream(prefix, key);
                        } else {
                            return fpStream.stream(got.getFp());
                        }
                    }),
                    fpKeyValueStream);
            } else {
                return keys.consume((prefix, key) -> {
                    WALPointer got = pointerIndex.get(new WALKey(prefix, key));
                    if (got == null) {
                        return fpKeyValueStream.stream(-1, prefix, key, null, -1, false);
                    } else {
                        return fpStream.stream(got.getFp());
                    }
                });
            }
        }, fpKeyValueStream);
    }

    boolean get(WALKeys keys, FpKeyValueStream fpKeyValueStream) throws Exception {
        return streamRawValues(keys::consume, fpKeyValueStream);
    }

    WALPointer getPointer(byte[] prefix, byte[] key) throws Exception {
        WALPointer got = pointerIndex.get(new WALKey(prefix, key));
        if (got != null) {
            return got;
        }
        PartitionDelta partitionDelta = merging.get();
        if (partitionDelta != null) {
            return partitionDelta.getPointer(prefix, key);
        }
        return null;
    }

    boolean getPointers(KeyValues keyValues, KeyValuePointerStream stream) throws Exception {
        return keyValues.consume((prefix, key, value, valueTimestamp, valueTombstone) -> {
            WALPointer pointer = getPointer(prefix, key);
            if (pointer != null) {
                return stream.stream(prefix, key, value, valueTimestamp, valueTombstone, pointer.getTimestampId(), pointer.getTombstoned(), pointer.getFp());
            } else {
                return stream.stream(prefix, key, value, valueTimestamp, valueTombstone, -1, false, -1);
            }
        });
    }

    Boolean containsKey(byte[] prefix, byte[] key) {
        WALPointer got = pointerIndex.get(new WALKey(prefix, key));
        if (got != null) {
            return !got.getTombstoned();
        }
        PartitionDelta partitionDelta = merging.get();
        if (partitionDelta != null) {
            return partitionDelta.containsKey(prefix, key);
        }
        return null;
    }

    boolean containsKeys(WALKeys keys, KeyTombstoneExistsStream stream) throws Exception {
        return keys.consume((prefix, key) -> {
            Boolean got = containsKey(prefix, key);
            return stream.stream(prefix, key, got != null && !got, got != null);
        });
    }

    interface KeyTombstoneExistsStream {

        boolean stream(byte[] prefix, byte[] key, boolean tombstoned, boolean exists) throws Exception;
    }

    void put(long fp,
        byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstone,
        WALHighwater highwater) {

        WALPointer pointer = new WALPointer(fp, valueTimestamp, valueTombstone);
        WALKey walKey = new WALKey(prefix, key);
        pointerIndex.put(walKey, pointer);
        orderedIndex.put(walKey.compose(), pointer);
    }

    private AtomicBoolean firstAndOnlyOnce = new AtomicBoolean(true);

    public boolean shouldWriteHighwater() {
        long got = updatesSinceLastHighwaterFlush.get();
        if (got > 1000) { // TODO expose to partition config
            updatesSinceLastHighwaterFlush.set(0);
            return true;
        } else {
            return firstAndOnlyOnce.compareAndSet(true, false);
        }
    }

    boolean keys(WALKeyStream keyStream) throws Exception {
        return WALKey.decomposeEntries(stream -> {
            for (byte[] pk : orderedIndex.keySet()) {
                if (!stream.stream(pk, null)) {
                    return false;
                }
            }
            return true;
        }, (prefix, key, entry) -> keyStream.stream(prefix, key));
    }

    DeltaPeekableElmoIterator rangeScanIterator(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey) {
        byte[] from = WALKey.compose(fromPrefix, fromKey);
        byte[] to = WALKey.compose(toPrefix, toKey);
        Iterator<Map.Entry<byte[], WALPointer>> iterator = orderedIndex.subMap(from, to).entrySet().iterator();
        Iterator<Map.Entry<byte[], WALPointer>> mergingIterator = Iterators.emptyIterator();
        PartitionDelta mergingPartitionDelta = merging.get();
        DeltaWAL mergingDeltaWAL = null;
        if (mergingPartitionDelta != null) {
            mergingIterator = mergingPartitionDelta.orderedIndex.subMap(from, to).entrySet().iterator();
            mergingDeltaWAL = mergingPartitionDelta.deltaWAL;
        }
        return new DeltaPeekableElmoIterator(iterator, mergingIterator, deltaWAL, mergingDeltaWAL);
    }

    DeltaPeekableElmoIterator rowScanIterator() {
        Iterator<Map.Entry<byte[], WALPointer>> iterator = orderedIndex.entrySet().iterator();
        Iterator<Map.Entry<byte[], WALPointer>> mergingIterator = Iterators.emptyIterator();
        PartitionDelta mergingPartitionDelta = merging.get();
        DeltaWAL mergingDeltaWAL = null;
        if (mergingPartitionDelta != null) {
            mergingIterator = mergingPartitionDelta.orderedIndex.entrySet().iterator();
            mergingDeltaWAL = mergingPartitionDelta.deltaWAL;
        }
        return new DeltaPeekableElmoIterator(iterator, mergingIterator, deltaWAL, mergingDeltaWAL);
    }

    long highestTxId() {
        if (txIdWAL.isEmpty()) {
            return -1;
        }
        return txIdWAL.lastKey();
    }

    public long lowestTxId() {
        PartitionDelta partitionDelta = merging.get();
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

    public boolean takeRowsFromTransactionId(long transactionId, RowStream rowStream) throws Exception {
        PartitionDelta partitionDelta = merging.get();
        if (partitionDelta != null) {
            if (!partitionDelta.takeRowsFromTransactionId(transactionId, rowStream)) {
                return false;
            }
        }

        if (txIdWAL.isEmpty() || txIdWAL.lastEntry().getKey() < transactionId) {
            return true;
        }

        ConcurrentNavigableMap<Long, long[]> tailMap = txIdWAL.tailMap(transactionId, false);
        return deltaWAL.takeRows(tailMap, rowStream);
    }

    long merge(PartitionIndex partitionIndex) throws Exception {
        final PartitionDelta merge = merging.get();
        long merged = 0;
        if (merge != null) {
            if (!merge.txIdWAL.isEmpty()) {
                merged = merge.size();
                try {
                    PartitionStore partitionStore = partitionIndex.get(merge.versionedPartitionName);
                    long highestTxId = partitionStore.highestTxId();
                    LOG.info("Merging ({}) deltas for partition: {} from tx: {}", merge.orderedIndex.size(), merge.versionedPartitionName, highestTxId);
                    LOG.debug("Merging keys: {}", merge.orderedIndex.keySet());
                    MutableBoolean eos = new MutableBoolean(false);
                    for (Map.Entry<Long, long[]> e : merge.txIdWAL.tailMap(highestTxId, true).entrySet()) {
                        long txId = e.getKey();
                        partitionStore.merge(txId,
                            (highwaters, scan) -> WALKey.decompose(
                                txFpRawKeyValueStream -> merge.deltaWAL.hydrateKeyValueHighwaters(
                                    fpStream -> {
                                        for (long fp : e.getValue()) {
                                            if (!fpStream.stream(fp)) {
                                                return false;
                                            }
                                        }
                                        return true;
                                    },
                                    (fp, prefix, key, value, valueTimestamp, valueTombstone, highwater) -> {
                                        // prefix is the partitionName and is discarded
                                        WALPointer pointer = merge.orderedIndex.get(key);
                                        if (pointer == null) {
                                            throw new RuntimeException("Delta WAL missing key: " + Arrays.toString(key));
                                        }
                                        if (pointer.getFp() == fp) {
                                            if (!txFpRawKeyValueStream.stream(txId, fp, key, value, valueTimestamp, valueTombstone)) {
                                                return false;
                                            }
                                            if (highwater != null) {
                                                highwaters.highwater(highwater);
                                            }
                                        }
                                        return true;
                                    }),
                                (_txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, row) -> {
                                    if (!scan.row(txId, prefix, key, value, valueTimestamp, valueTombstoned)) {
                                        eos.setValue(true);
                                        return false;
                                    }
                                    return true;
                                }));
                        if (eos.booleanValue()) {
                            break;
                        }
                    }
                    partitionStore.getWalStorage().commitIndex();
                    LOG.info("Merged deltas for {}", merge.versionedPartitionName);
                } catch (Throwable ex) {
                    throw new RuntimeException("Error while streaming entry set.", ex);
                }
            }
        }
        merging.set(null);
        return merged;
    }
}
