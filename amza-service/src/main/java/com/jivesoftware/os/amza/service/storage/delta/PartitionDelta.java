package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.api.stream.KeyValues;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.WALKeyPointerStream;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALPointer;
import com.jivesoftware.os.amza.api.wal.WALPrefix;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Comparator;
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

    private final ConcurrentBAHash<WALPointer> pointerIndex = new ConcurrentBAHash<>(3, true, 4);
    private final ConcurrentSkipListMap<byte[], WALPointer> orderedIndex = new ConcurrentSkipListMap<>(KeyUtil::compare);
    private final ConcurrentHashMap<WALPrefix, AppendOnlyConcurrentArrayList> prefixTxFpIndex = new ConcurrentHashMap<>();
    private final AppendOnlyConcurrentArrayList txIdWAL = new AppendOnlyConcurrentArrayList(11); //TODO expose to config
    private final AtomicLong updatesSinceLastHighwaterFlush = new AtomicLong();

    PartitionDelta(VersionedPartitionName versionedPartitionName,
        DeltaWAL deltaWAL,
        PartitionDelta merging) {
        this.versionedPartitionName = versionedPartitionName;
        this.deltaWAL = deltaWAL;
        this.merging = new AtomicReference<>(merging);
    }

    public long getDeltaWALId() {
        return deltaWAL.getId();
    }

    public long getPrevDeltaWALId() {
        return deltaWAL.getPrevId();
    }

    public long size() {
        return pointerIndex.size();
    }

    private boolean streamRawValues(byte[] prefix, UnprefixedWALKeys keys, FpKeyValueStream fpKeyValueStream) throws Exception {
        return deltaWAL.hydrate(fpStream -> {
            PartitionDelta mergingPartitionDelta = merging.get();
            if (mergingPartitionDelta != null) {
                return mergingPartitionDelta.streamRawValues(prefix,
                    mergingKeyStream -> keys.consume((key) -> {
                        WALPointer got = pointerIndex.get(WALKey.compose(prefix, key));
                        if (got == null) {
                            return mergingKeyStream.stream(key);
                        } else {
                            return fpStream.stream(got.getFp());
                        }
                    }),
                    fpKeyValueStream);
            } else {
                return keys.consume((key) -> {
                    WALPointer got = pointerIndex.get(WALKey.compose(prefix, key));
                    if (got == null) {
                        return fpKeyValueStream.stream(-1, null, prefix, key, null, -1, false, -1);
                    } else {
                        return fpStream.stream(got.getFp());
                    }
                });
            }
        }, fpKeyValueStream);
    }

    boolean get(byte[] prefix, UnprefixedWALKeys keys, FpKeyValueStream fpKeyValueStream) throws Exception {
        return streamRawValues(prefix, keys::consume, fpKeyValueStream);
    }

    WALPointer getPointer(byte[] prefix, byte[] key) throws Exception {
        WALPointer got = pointerIndex.get(WALKey.compose(prefix, key));
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
        return keyValues.consume((rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
            WALPointer pointer = getPointer(prefix, key);
            if (pointer != null) {
                return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion,
                    pointer.getTimestampId(), pointer.getTombstoned(), pointer.getVersion(), pointer.getFp());
            } else {
                return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion, -1, false, -1, -1);
            }
        });
    }

    Boolean containsKey(byte[] prefix, byte[] key) {
        WALPointer got = pointerIndex.get(WALKey.compose(prefix, key));
        if (got != null) {
            return !got.getTombstoned();
        }
        PartitionDelta partitionDelta = merging.get();
        if (partitionDelta != null) {
            return partitionDelta.containsKey(prefix, key);
        }
        return null;
    }

    boolean containsKeys(byte[] prefix, UnprefixedWALKeys keys, KeyTombstoneExistsStream stream) throws Exception {
        return keys.consume((key) -> {
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
        long valueTimestamp,
        boolean valueTombstone,
        long valueVersion) {

        WALPointer pointer = new WALPointer(fp, valueTimestamp, valueTombstone, valueVersion);
        byte[] walKey = WALKey.compose(prefix, key);
        pointerIndex.put(walKey, pointer);
        orderedIndex.put(walKey, pointer);
    }

    private final AtomicBoolean firstAndOnlyOnce = new AtomicBoolean(true);

    public boolean shouldWriteHighwater() {
        long got = updatesSinceLastHighwaterFlush.get();
        if (got > 1000) { // TODO expose to partition config
            updatesSinceLastHighwaterFlush.set(0);
            return true;
        } else {
            return firstAndOnlyOnce.compareAndSet(true, false);
        }
    }

    boolean keys(WALKeyPointerStream keyPointerStream) throws Exception {
        return WALKey.decompose(
            txFpRawKeyValueEntryStream -> {
                for (Map.Entry<byte[], WALPointer> entry : orderedIndex.entrySet()) {
                    WALPointer pointer = entry.getValue();
                    if (!txFpRawKeyValueEntryStream.stream(-1,
                        pointer.getFp(),
                        null,
                        entry.getKey(),
                        null,
                        pointer.getTimestampId(),
                        pointer.getTombstoned(),
                        pointer.getVersion(),
                        null)) {
                        return false;
                    }
                }
                return true;
            },
            (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, entry)
                -> keyPointerStream.stream(prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp));
    }

    DeltaPeekableElmoIterator rangeScanIterator(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey) {
        byte[] from = fromKey != null ? WALKey.compose(fromPrefix, fromKey) : null;
        byte[] to = toKey != null ? WALKey.compose(toPrefix, toKey) : null;
        Iterator<Map.Entry<byte[], WALPointer>> iterator = subMap(orderedIndex, from, to).entrySet().iterator();
        Iterator<Map.Entry<byte[], WALPointer>> mergingIterator = Iterators.emptyIterator();
        PartitionDelta mergingPartitionDelta = merging.get();
        DeltaWAL mergingDeltaWAL = null;
        if (mergingPartitionDelta != null) {
            mergingIterator = subMap(mergingPartitionDelta.orderedIndex, from, to).entrySet().iterator();
            mergingDeltaWAL = mergingPartitionDelta.deltaWAL;
        }
        return new DeltaPeekableElmoIterator(iterator, mergingIterator, deltaWAL, mergingDeltaWAL);
    }

    private static ConcurrentNavigableMap<byte[], WALPointer> subMap(ConcurrentSkipListMap<byte[], WALPointer> index, byte[] from, byte[] to) {
        if (from != null && to != null) {
            if (KeyUtil.compare(from, to) <= 0) {
                return index.subMap(from, to);
            } else {
                return index.subMap(from, to).descendingMap();
            }
        } else if (from != null) {
            return index.tailMap(from, true);
        } else if (to != null) {
            return index.headMap(to, false);
        } else {
            return index;
        }
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
            PartitionDelta partitionDelta = merging.get();
            return (partitionDelta != null) ? partitionDelta.highestTxId() : -1;
        }
        return txIdWAL.last().txId;
    }

    long highestTxId(byte[] prefix) {
        AppendOnlyConcurrentArrayList prefixTxFps = prefixTxFpIndex.get(new WALPrefix(prefix));
        if (prefixTxFps == null || prefixTxFps.isEmpty()) {
            PartitionDelta partitionDelta = merging.get();
            return (partitionDelta != null) ? partitionDelta.highestTxId(prefix) : -1;
        }
        return prefixTxFps.last().txId;
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
        return txIdWAL.first().txId;
    }

    public long lowestTxId(byte[] prefix) {
        PartitionDelta partitionDelta = merging.get();
        if (partitionDelta != null) {
            long lowestTxId = partitionDelta.lowestTxId(prefix);
            if (lowestTxId >= 0) {
                return lowestTxId;
            }
        }

        AppendOnlyConcurrentArrayList prefixTxFps = prefixTxFpIndex.get(new WALPrefix(prefix));
        if (prefixTxFps == null || prefixTxFps.isEmpty()) {
            return -1;
        }
        return prefixTxFps.first().txId;
    }

    void onLoadAppendTxFp(byte[] prefix, long rowTxId, long rowFP) {
        if (txIdWAL.isEmpty() || txIdWAL.last().txId != rowTxId) {
            txIdWAL.add(new TxFps(prefix, rowTxId, new long[] { rowFP }));
        } else {
            txIdWAL.onLoadAddFpToTail(rowFP);
        }
        if (prefix != null) {
            AppendOnlyConcurrentArrayList prefixTxFps = prefixTxFpIndex.computeIfAbsent(new WALPrefix(prefix),
                walPrefix -> new AppendOnlyConcurrentArrayList(8));
            if (prefixTxFps.isEmpty() || prefixTxFps.last().txId != rowTxId) {
                prefixTxFps.add(new TxFps(prefix, rowTxId, new long[] { rowFP }));
            } else {
                prefixTxFps.onLoadAddFpToTail(rowFP);
            }
        }
    }

    void appendTxFps(byte[] prefix, long rowTxId, long[] rowFPs) {
        TxFps txFps = new TxFps(prefix, rowTxId, rowFPs);
        if (prefix != null) {
            AppendOnlyConcurrentArrayList prefixTxFps = prefixTxFpIndex.computeIfAbsent(new WALPrefix(prefix),
                walPrefix -> new AppendOnlyConcurrentArrayList(8));
            prefixTxFps.add(txFps);
        }

        txIdWAL.add(txFps);
        updatesSinceLastHighwaterFlush.addAndGet(rowFPs.length);
    }

    public boolean takeRowsFromTransactionId(long transactionId, RowStream rowStream) throws Exception {
        PartitionDelta partitionDelta = merging.get();
        if (partitionDelta != null) {
            if (!partitionDelta.takeRowsFromTransactionId(transactionId, rowStream)) {
                return false;
            }
        }

        if (txIdWAL.isEmpty() || txIdWAL.last().txId < transactionId) {
            return true;
        }

        return deltaWAL.takeRows(txFpsStream -> txIdWAL.streamFromTxId(transactionId, false, txFpsStream), rowStream);
    }

    public boolean takeRowsFromTransactionId(byte[] prefix, long transactionId, RowStream rowStream) throws Exception {
        PartitionDelta partitionDelta = merging.get();
        if (partitionDelta != null) {
            if (!partitionDelta.takeRowsFromTransactionId(prefix, transactionId, rowStream)) {
                return false;
            }
        }

        AppendOnlyConcurrentArrayList prefixTxFps = prefixTxFpIndex.get(new WALPrefix(prefix));
        if (prefixTxFps == null || prefixTxFps.isEmpty() || prefixTxFps.last().txId < transactionId) {
            return true;
        }

        return deltaWAL.takeRows(txFpsStream -> prefixTxFps.streamFromTxId(transactionId, false, txFpsStream), rowStream);
    }

    public static class MergeResult {

        public final VersionedPartitionName versionedPartitionName;
        public final WALIndex walIndex;
        public final long count;
        public final long lastTxId;

        public MergeResult(VersionedPartitionName versionedPartitionName, WALIndex walIndex, long count, long lastTxId) {
            this.versionedPartitionName = versionedPartitionName;
            this.walIndex = walIndex;
            this.count = count;
            this.lastTxId = lastTxId;
        }
    }

    MergeResult merge(PartitionIndex partitionIndex, boolean validate) throws Exception {
        final PartitionDelta merge = merging.get();
        long merged = 0;
        long lastTxId = 0;
        WALIndex walIndex = null;
        if (merge != null) {
            if (!merge.txIdWAL.isEmpty()) {
                merged = merge.size();
                lastTxId = merge.highestTxId();

                PartitionStore partitionStore = validate
                    ? partitionIndex.getAndValidate(merge.getDeltaWALId(), merge.getPrevDeltaWALId(), merge.versionedPartitionName)
                    : partitionIndex.get(merge.versionedPartitionName);
                PartitionProperties properties = partitionIndex.getProperties(merge.versionedPartitionName.getPartitionName());
                long highestTxId = partitionStore.highestTxId();
                LOG.info("Merging ({}) deltas for partition: {} from tx: {}", merge.pointerIndex.size(), merge.versionedPartitionName, highestTxId);
                LOG.debug("Merging keys: {}", merge.orderedIndex.keySet());
                MutableBoolean eos = new MutableBoolean(false);
                merge.txIdWAL.streamFromTxId(highestTxId, true, txFps -> {
                    long txId = txFps.txId;

                    partitionStore.merge(false,
                        properties,
                        txId,
                        txFps.prefix,
                        (highwaters, scan) -> WALKey.decompose(
                            txFpRawKeyValueStream -> merge.deltaWAL.hydrateKeyValueHighwaters(
                                fpStream -> {
                                    for (long fp : txFps.fps) {
                                        if (!fpStream.stream(fp)) {
                                            return false;
                                        }
                                    }
                                    return true;
                                },
                                (fp, rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion, highwater) -> {
                                    // prefix is the partitionName and is discarded
                                    WALPointer pointer = merge.orderedIndex.get(key);
                                    if (pointer == null) {
                                        throw new RuntimeException("Delta WAL missing"
                                            + " prefix: " + Arrays.toString(prefix)
                                            + " key: " + Arrays.toString(key)
                                            + " for: " + versionedPartitionName);
                                    }
                                    if (pointer.getFp() == fp) {
                                        if (!txFpRawKeyValueStream.stream(txId, fp, rowType, key, value, valueTimestamp, valueTombstone, valueVersion,
                                            null)) {
                                            return false;
                                        }
                                        if (highwater != null) {
                                            highwaters.highwater(highwater);
                                        }
                                    }
                                    return true;
                                }),
                            (_txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row) -> {
                                if (!scan.row(txId, key, value, valueTimestamp, valueTombstoned, valueVersion)) {
                                    eos.setValue(true);
                                    return false;
                                }
                                return true;
                            }));
                    return !eos.booleanValue();
                });
                partitionStore.getWalStorage().endOfMergeMarker(merge.getDeltaWALId(), lastTxId);
                walIndex = partitionStore.getWalStorage().commitIndex(true, lastTxId);
                LOG.info("Merged deltas for {}", merge.versionedPartitionName);
            }
        }
        merging.set(null);
        return new MergeResult(versionedPartitionName, walIndex, merged, lastTxId);
    }

    private static final Comparator<TxFps> txFpsComparator = (o1, o2) -> Longs.compare(o1.txId, o2.txId);

    private static class AppendOnlyConcurrentArrayList {

        private volatile TxFps[] array;
        private volatile int length;

        public AppendOnlyConcurrentArrayList(int initialCapacity) {
            this.array = new TxFps[Math.max(initialCapacity, 1)];
        }

        public void onLoadAddFpToTail(long fp) {
            long[] existing = array[length - 1].fps;
            long[] extended = new long[existing.length + 1];
            System.arraycopy(existing, 0, extended, 0, existing.length);
            extended[existing.length] = fp;
            array[length - 1].fps = extended;
        }

        public void add(TxFps txFps) {
            synchronized (this) {
                if (length > 0 && txFps.txId <= array[length - 1].txId) {
                    throw new IllegalStateException("Appending txIds out of order: " + txFps.txId + " <= " + array[length - 1].txId);
                }
                if (length == array.length) {
                    TxFps[] doubled = new TxFps[array.length * 2];
                    System.arraycopy(array, 0, doubled, 0, array.length);
                    array = doubled;
                }
                array[length] = txFps;
                length++;
            }
        }

        public boolean streamFromTxId(long txId, boolean inclusive, TxFpsStream txFpsStream) throws Exception {
            TxFps[] array;
            int length;
            synchronized (this) {
                array = this.array;
                length = this.length;
            }
            int index = Arrays.binarySearch(array, 0, length, new TxFps(null, txId, null), txFpsComparator);
            if (index >= 0 && !inclusive) {
                index++;
            } else if (index < 0) {
                index = -(index + 1);
            }
            while (true) {
                for (int i = index; i < length; i++) {
                    if (!txFpsStream.stream(array[i])) {
                        return false;
                    }
                }
                int latestLength;
                synchronized (this) {
                    latestLength = this.length;
                    array = this.array;
                }
                if (latestLength != length) {
                    index = length;
                    length = latestLength;
                } else {
                    break;
                }
            }
            return true;
        }

        public boolean isEmpty() {
            synchronized (this) {
                return length == 0;
            }
        }

        public TxFps first() {
            return array[0];
        }

        public TxFps last() {
            TxFps[] array;
            int length;
            synchronized (this) {
                array = this.array;
                length = this.length;
            }
            return array[length - 1];
        }
    }
}
