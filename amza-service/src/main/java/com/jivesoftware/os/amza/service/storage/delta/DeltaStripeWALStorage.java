package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.delta.DeltaWAL.KeyValueHighwater;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RangeScannable;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scannable;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.TimestampKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
import com.jivesoftware.os.amza.shared.wal.WALTimestampId;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class DeltaStripeWALStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config

    private final int index;
    private final AmzaStats amzaStats;
    private final DeltaWALFactory deltaWALFactory;
    private final AtomicReference<DeltaWAL> deltaWAL = new AtomicReference<>();
    private final DeltaValueCache deltaValueCache;
    private final Object awakeCompactionsLock = new Object();
    private final long compactAfterNUpdates;
    private final ConcurrentHashMap<VersionedPartitionName, PartitionDelta> partitionDeltas = new ConcurrentHashMap<>();
    private final Object oneWriterAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final ExecutorService compactionThreads;
    private final AtomicLong updateSinceLastCompaction = new AtomicLong();
    private final AtomicBoolean compacting = new AtomicBoolean(false);

    private final Reentrant reentrant = new Reentrant();

    static class Reentrant extends ThreadLocal<Integer> {

        @Override
        protected Integer initialValue() {
            return 0;
        }
    }

    public DeltaStripeWALStorage(int index,
        AmzaStats amzaStats,
        DeltaWALFactory deltaWALFactory,
        DeltaValueCache deltaValueCache,
        long compactAfterNUpdates) {

        this.index = index;
        this.amzaStats = amzaStats;
        this.deltaWALFactory = deltaWALFactory;
        this.deltaValueCache = deltaValueCache;
        this.compactAfterNUpdates = compactAfterNUpdates;
        int numberOfCompactorThreads = 1; // TODO expose to config;
        this.compactionThreads = Executors.newFixedThreadPool(numberOfCompactorThreads,
            new ThreadFactoryBuilder().setNameFormat("compact-deltas-" + index + "-%d").build());
    }

    private void acquireOne() throws InterruptedException {
        int enters = reentrant.get();
        if (enters == 0) {
            tickleMeElmophore.acquire();
        }
        reentrant.set(enters + 1);
    }

    private void releaseOne() {
        int enters = reentrant.get();
        if (enters - 1 == 0) {
            tickleMeElmophore.release();
        }
        reentrant.set(enters - 1);
    }

    private void acquireAll() throws InterruptedException {
        tickleMeElmophore.acquire(numTickleMeElmaphore);
    }

    private void releaseAll() {
        tickleMeElmophore.release(numTickleMeElmaphore);
    }

    public Object getAwakeCompactionLock() {
        return awakeCompactionsLock;
    }

    public boolean expunge(VersionedPartitionName versionedPartitionName, WALStorage walStorage) throws Exception {
        acquireAll();
        try {
            partitionDeltas.remove(versionedPartitionName);
            return true;
        } finally {
            releaseAll();
        }
    }

    public void load(PartitionIndex partitionIndex, PrimaryRowMarshaller<byte[]> primaryRowMarshaller) throws Exception {
        LOG.info("Reloading deltas...");
        long start = System.currentTimeMillis();
        synchronized (oneWriterAtATimeLock) {
            List<DeltaWAL> deltaWALs = deltaWALFactory.list();
            if (deltaWALs.isEmpty()) {
                deltaWAL.set(deltaWALFactory.create());
            } else {
                for (int i = 0; i < deltaWALs.size(); i++) {
                    final DeltaWAL wal = deltaWALs.get(i);
                    if (i > 0) {
                        compactDelta(partitionIndex, deltaWAL.get(), () -> wal);
                    }
                    deltaWAL.set(wal);
                    wal.load((long rowFP, final long rowTxId, RowType rowType, byte[] rawRow) -> {
                        if (rowType == RowType.primary) {
                            primaryRowMarshaller.fromRow(rawRow, (key, value, valueTimestamp, valueTombstoned) -> {
                                ByteBuffer bb = ByteBuffer.wrap(key);
                                byte[] partitionNameBytes = new byte[bb.getShort()];
                                bb.get(partitionNameBytes);
                                final byte[] keyBytes = new byte[bb.getInt()];
                                bb.get(keyBytes);

                                VersionedPartitionName versionedPartitionName = VersionedPartitionName.fromBytes(partitionNameBytes);
                                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                                if (partitionStore == null) {
                                    LOG.warn("Dropping values on the floor for versionedPartitionName:{} "
                                        + " this is typical when loading an expunged partition", versionedPartitionName);
                                    // TODO ensure partitionIsExpunged?
                                } else {
                                    acquireOne();
                                    try {
                                        PartitionDelta delta = getPartitionDelta(versionedPartitionName);
                                        WALValue partitionValue = partitionStore.get(keyBytes);
                                        if (partitionValue == null || partitionValue.getTimestampId() < valueTimestamp) {
                                            WALPointer got = delta.getPointer(keyBytes);
                                            if (got == null || got.getTimestampId() < valueTimestamp) {
                                                wal.hydrateKeyValueHighwater(rowFP,
                                                    (fp, key1, value1, valueTimestamp1, valueTombstone, highwater) -> {
                                                        delta.put(fp, key1, value1, valueTimestamp1, valueTombstone, highwater);
                                                        delta.appendTxFps(rowTxId, fp);
                                                        return true;
                                                    });

                                            }
                                        }
                                    } finally {
                                        releaseOne();
                                    }
                                }
                                updateSinceLastCompaction.incrementAndGet();
                                return true;
                            });

                        }
                        return true;
                    });

                }
            }
        }
        if (updateSinceLastCompaction.get() > compactAfterNUpdates) {
            synchronized (awakeCompactionsLock) {
                awakeCompactionsLock.notifyAll();
            }
        }
        LOG.info("Reloaded deltas stripe:{} in {} ms", index, (System.currentTimeMillis() - start));
    }

    public void flush(boolean fsync) throws Exception {
        DeltaWAL wal = deltaWAL.get();
        if (wal != null) {
            wal.flush(fsync);
        }
    }

    public long getHighestTxId(VersionedPartitionName versionedPartitionName, WALStorage storage) throws Exception {
        PartitionDelta partitionDelta = partitionDeltas.get(versionedPartitionName);
        if (partitionDelta != null) {
            return partitionDelta.highestTxId();
        }
        return storage.highestTxId();
    }

    // todo any one call this should have atleast 1 numTickleMeElmaphore
    private PartitionDelta getPartitionDelta(VersionedPartitionName versionedPartitionName) {
        PartitionDelta partitionDelta = partitionDeltas.get(versionedPartitionName);
        if (partitionDelta == null) {
            DeltaWAL wal = deltaWAL.get();
            if (wal == null) {
                throw new IllegalStateException("Delta WAL is currently unavailable.");
            }
            partitionDelta = new PartitionDelta(versionedPartitionName, wal, deltaValueCache, null);
            PartitionDelta had = partitionDeltas.putIfAbsent(versionedPartitionName, partitionDelta);
            if (had != null) {
                partitionDelta = had;
            }
        }
        return partitionDelta;
    }

    public boolean compactable() {
        return updateSinceLastCompaction.get() > compactAfterNUpdates;
    }

    public void compact(PartitionIndex partitionIndex, boolean force) throws Exception {
        if (!force && !compactable()) {
            return;
        }

        if (!compacting.compareAndSet(false, true)) {
            LOG.warn("Trying to compact DeltaStripe:" + partitionIndex + " while another compaction is already in progress.");
            return;
        }
        amzaStats.beginCompaction("Compacting Delta Stripe:" + index);
        try {
            updateSinceLastCompaction.set(0);
            compactDelta(partitionIndex, deltaWAL.get(), deltaWALFactory::create);
        } finally {
            compacting.set(false);
            amzaStats.endCompaction("Compacting Delta Stripe:" + index);
        }
    }

    private void compactDelta(final PartitionIndex partitionIndex, DeltaWAL wal, Callable<DeltaWAL> newWAL) throws Exception {
        final List<Future<Boolean>> futures = new ArrayList<>();
        acquireAll();
        try {
            for (Map.Entry<VersionedPartitionName, PartitionDelta> e : partitionDeltas.entrySet()) {
                if (e.getValue().compacting.get() != null) {
                    LOG.warn("Ingress is faster than we can compact!");
                    return;
                }
            }
            LOG.info("Compacting delta partitions...");
            DeltaWAL newDeltaWAL = newWAL.call();
            deltaWAL.set(newDeltaWAL);
            for (Map.Entry<VersionedPartitionName, PartitionDelta> e : partitionDeltas.entrySet()) {
                PartitionDelta partitionDelta = new PartitionDelta(e.getKey(),
                    newDeltaWAL,
                    deltaValueCache,
                    e.getValue());
                partitionDeltas.put(e.getKey(), partitionDelta);
                futures.add(compactionThreads.submit(() -> {
                    try {
                        partitionDelta.compact(partitionIndex);
                        return true;
                    } catch (Exception x) {
                        LOG.error("Failed to compact:" + partitionDelta, x);
                        return false;
                    }
                }));
            }
        } finally {
            releaseAll();
        }
        boolean failed = false;
        for (Future<Boolean> f : futures) {
            Boolean success = f.get();
            if (success != null && !success) {
                failed = true;
            }
        }
        acquireAll();
        try {
            if (!failed) {
                wal.destroy();
                LOG.info("Compacted delta partitions.");
            } else {
                LOG.warn("Compaction of delta partition FAILED.");
            }
        } finally {
            releaseAll();
        }
    }

    public RowsChanged update(HighwaterStorage highwaterStorage,
        VersionedPartitionName versionedPartitionName,
        Status partitionStatus,
        WALStorage storage,
        Commitable updates,
        WALUpdated updated) throws Exception {

        final AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);

        final Table<Long, WALKey, WALValue> apply = Tables.newCustomTable(new LinkedHashMap<>(), LinkedHashMap::new);

        final Map<WALKey, WALTimestampId> removes = new HashMap<>();
        final Map<WALKey, WALTimestampId> clobbers = new HashMap<>();

        final List<byte[]> keys = new ArrayList<>();
        final List<WALValue> values = new ArrayList<>();
        updates.commitable(null, (transactionId, key, value, valueTimestamp, valueTombstone) -> {
            keys.add(key);
            values.add(new WALValue(value, valueTimestamp, valueTombstone));
            return true;
        });

        acquireOne();
        try {
            DeltaWAL wal = deltaWAL.get();
            RowsChanged rowsChanged;

            getTimestamps(versionedPartitionName, storage, (stream) -> {
                for (int i = 0; i < keys.size(); i++) {
                    byte[] key = keys.get(i);
                    WALValue update = values.get(i);
                    if (!stream.stream(key, update.getValue(), update.getTimestampId(), update.getTombstoned())) {
                        return false;
                    }
                }
                return true;
            }, (byte[] key, byte[] value, long valueTimestamp, boolean valueTombstone, long pointerTimestamp, boolean pointerTombstoned, long pointerFp) -> {
                WALKey walKey = new WALKey(key);
                WALValue walValue = new WALValue(value, valueTimestamp, valueTombstone);
                if (pointerFp == -1) {
                    apply.put(-1L, walKey, walValue);
                    if (oldestAppliedTimestamp.get() > valueTimestamp) {
                        oldestAppliedTimestamp.set(valueTimestamp);
                    }
                } else if (pointerTimestamp < valueTimestamp) {
                    apply.put(-1L, walKey, walValue);
                    if (oldestAppliedTimestamp.get() > valueTimestamp) {
                        oldestAppliedTimestamp.set(valueTimestamp);
                    }
                    WALTimestampId walTimestampId = new WALTimestampId(pointerTimestamp, pointerTombstoned);
                    clobbers.put(walKey, walTimestampId);
                    if (valueTombstone && !pointerTombstoned) {
                        removes.put(walKey, walTimestampId);
                    }
                }
                return true;
            });

            if (apply.isEmpty()) {
                rowsChanged = new RowsChanged(versionedPartitionName, oldestAppliedTimestamp.get(), HashBasedTable.create(), removes, clobbers, -1);
            } else {
                PartitionDelta delta = getPartitionDelta(versionedPartitionName);
                WALHighwater partitionHighwater = null;
                if (delta.shouldWriteHighwater()) {
                    partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
                    LOG.inc("highwaterHint", 1);
                    LOG.inc("highwaterHint", 1, versionedPartitionName.getPartitionName().getName());
                }
                DeltaWAL.DeltaWALApplied updateApplied;
                synchronized (oneWriterAtATimeLock) {
                    updateApplied = wal.update(versionedPartitionName, apply.row(-1L), partitionHighwater);

                    for (int i = 0; i < updateApplied.fps.length; i++) {
                        KeyValueHighwater keyValueHighwater = updateApplied.keyValueHighwaters[i];
                        long fp = updateApplied.fps[i];
                        WALPointer got = delta.getPointer(keyValueHighwater.key);
                        if (got == null || got.getTimestampId() < keyValueHighwater.valueTimestamp) {
                            delta.put(fp, keyValueHighwater.key, keyValueHighwater.value, keyValueHighwater.valueTimestamp, keyValueHighwater.valueTombstone,
                                keyValueHighwater.highwater);
                        } else {
                            apply.remove(-1L, keyValueHighwater.key);
                        }
                    }
                    delta.appendTxFps(updateApplied.txId, updateApplied.fps);
                    rowsChanged = new RowsChanged(versionedPartitionName,
                        oldestAppliedTimestamp.get(),
                        apply,
                        removes,
                        clobbers,
                        updateApplied.txId);
                }
                updated.updated(versionedPartitionName, partitionStatus, updateApplied.txId);
            }

            long uncompactedUpdates = updateSinceLastCompaction.addAndGet(apply.size());
            if (uncompactedUpdates > compactAfterNUpdates) {
                synchronized (awakeCompactionsLock) {
                    awakeCompactionsLock.notifyAll();
                }
            }
            return rowsChanged;
        } finally {
            releaseOne();
        }
    }

    public void takeRowUpdatesSince(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        long transactionId,
        RowStream rowStream) throws Exception {

        long lowestTxId;
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            lowestTxId = delta.lowestTxId();
        } finally {
            releaseOne();
        }

        if ((lowestTxId == -1 || lowestTxId > transactionId) && !storage.takeRowsFromTransactionId(transactionId, rowStream)) {
            return;
        }

        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            delta.takeRowUpdatesSince(transactionId, rowStream);
        } finally {
            releaseOne();
        }
    }

    public boolean takeRowsFromTransactionId(VersionedPartitionName versionedPartitionName, WALStorage storage, long transactionId, RowStream rowStream)
        throws Exception {

        long lowestTxId;
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            lowestTxId = delta.lowestTxId();
        } finally {
            releaseOne();
        }

        if ((lowestTxId == -1 || lowestTxId > transactionId) && !storage.takeRowsFromTransactionId(transactionId, rowStream)) {
            return false;
        }

        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            return delta.takeRowsFromTransactionId(transactionId, rowStream);
        } finally {
            releaseOne();
        }
    }

    public WALValue get(VersionedPartitionName versionedPartitionName, WALStorage storage, byte[] key) throws Exception {
        WALValue[] walValue = new WALValue[1];
        get(versionedPartitionName, storage, (stream) -> {
            return stream.stream(key, null, -1, false);
        }, (byte[] key1, byte[] value, long timestamp) -> {
            if (value != null) {
                walValue[0] = new WALValue(value, timestamp, false);
            }
            return true;
        });
        return walValue[0];
    }

    public boolean get(VersionedPartitionName versionedPartitionName, WALStorage storage, KeyValues keyValues, TimestampKeyValueStream stream) throws Exception {
        acquireOne();
        try {
            PartitionDelta partitionDelta = getPartitionDelta(versionedPartitionName);
            return storage.get((storageStream) -> {

                return partitionDelta.get(keyValues, (key, value, valueTimestamp, valueTombstoned) -> {
                    if (value == null) {
                        return storageStream.stream(key, null, -1, false);
                    } else {
                        if (valueTombstoned) {
                            return stream.stream(key, null, -1);
                        } else {
                            return stream.stream(key, value, valueTimestamp);
                        }
                    }
                });

            }, (key, value, valueTimestamp, valueTombstoned) -> {
                if (valueTombstoned) {
                    return stream.stream(key, null, -1);
                } else {
                    return stream.stream(key, value, valueTimestamp);
                }
            });
        } finally {
            releaseOne();
        }
    }

    public boolean containsKey(VersionedPartitionName versionedPartitionName, WALStorage storage, WALKey key) throws Exception {
        acquireOne();
        try {
            Boolean contained = getPartitionDelta(versionedPartitionName).containsKey(key);
            if (contained != null) {
                return contained;
            }
        } finally {
            releaseOne();
        }
        return storage.containsKey(key);
    }

    private boolean getTimestamps(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        KeyValues keyValues,
        WALKeyValuePointerStream stream) throws Exception {

        return storage.streamWALPointers((storageStream) -> {
            PartitionDelta partitionDelta = getPartitionDelta(versionedPartitionName);
            return partitionDelta.getPointers(keyValues, (key, value, valueTimestamp, valueTombstoned,
                pointerTimestamp, pointerTombstoned, pointerFp) -> {
                    if (pointerFp == -1) {
                        return storageStream.stream(key, value, valueTimestamp, valueTombstoned);
                    } else {
                        return stream.stream(key, value, valueTimestamp, valueTombstoned, pointerTimestamp, pointerTombstoned, pointerFp);
                    }
                });
        }, (key, value, valueTimestamp, valueTombstoned, pointerTimestamp, pointerTombstoned, pointerFp) -> {
            if (pointerFp == -1) {
                return stream.stream(key, value, valueTimestamp, valueTombstoned, -1, false, -1);
            } else {
                return stream.stream(key, value, valueTimestamp, valueTombstoned, pointerTimestamp, pointerTombstoned, pointerFp);
            }
        });

    }

    public void rangeScan(VersionedPartitionName versionedPartitionName,
        RangeScannable rangeScannable,
        WALKey from,
        WALKey to,
        KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            final DeltaPeekableElmoIterator iterator = delta.rangeScanIterator(from, to);
            rangeScannable.rangeScan(from, to, new LatestWinnernator(iterator, keyValueStream));

            Map.Entry<WALKey, WALValue> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    WALValue got = d.getValue();
                    keyValueStream.stream(d.getKey().getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned());
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    WALValue got = d.getValue();
                    if (!keyValueStream.stream(d.getKey().getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned())) {
                        return;
                    }
                }
            }
        } finally {
            releaseOne();
        }
    }

    public boolean rowScan(final VersionedPartitionName versionedPartitionName, Scannable scanable, KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            DeltaPeekableElmoIterator iterator = delta.rowScanIterator();
            if (!scanable.rowScan(new LatestWinnernator(iterator, keyValueStream))) {
                return false;
            }

            Map.Entry<WALKey, WALValue> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    WALValue got = d.getValue();
                    if (!keyValueStream.stream(d.getKey().getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned())) {
                        return false;
                    }
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    WALValue got = d.getValue();
                    if (!keyValueStream.stream(d.getKey().getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned())) {
                        return false;
                    }
                }
            }
            return true;
        } finally {
            releaseOne();
        }
    }

    /**
     * Stupid expensive!!!!
     */
    public long count(VersionedPartitionName versionedPartitionName, WALStorage storage) throws Exception {
        int count = 0;
        acquireOne();
        try {
            ArrayList<WALKey> keys = new ArrayList<>(getPartitionDelta(versionedPartitionName).keySet());
            List<Boolean> containsKey = storage.containsKey(keys);
            count = Iterables.frequency(containsKey, Boolean.FALSE);
        } finally {
            releaseOne();
        }
        return count + storage.count();
    }

    static class LatestWinnernator implements KeyValueStream {

        private final DeltaPeekableElmoIterator iterator;
        private final KeyValueStream keyValueStream;
        private Map.Entry<WALKey, WALValue> d;

        public LatestWinnernator(DeltaPeekableElmoIterator iterator, KeyValueStream keyValueStream) {
            this.iterator = iterator;
            this.keyValueStream = keyValueStream;
        }

        @Override
        public boolean stream(byte[] key, byte[] value, long valueTimestamp, boolean valueTombstone) throws Exception {
            if (d == null && iterator.hasNext()) {
                d = iterator.next();
            }
            boolean needsKey = true;
            while (d != null && UnsignedBytes.lexicographicalComparator().compare(d.getKey().getKey(), key) <= 0) {
                WALValue got = d.getValue();
                if (Arrays.equals(d.getKey().getKey(), key)) {
                    needsKey = false;
                }
                if (!keyValueStream.stream(d.getKey().getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned())) {
                    return false;
                }
                if (iterator.hasNext()) {
                    d = iterator.next();
                } else {
                    iterator.eos();
                    d = null;
                    break;
                }
            }
            if (needsKey) {
                return keyValueStream.stream(key, value, valueTimestamp, valueTombstone);
            } else {
                return true;
            }
        }
    }

}
