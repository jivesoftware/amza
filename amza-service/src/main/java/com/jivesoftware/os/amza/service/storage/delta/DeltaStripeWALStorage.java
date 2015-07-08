package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
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
import com.jivesoftware.os.amza.shared.scan.Scan;
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
import com.jivesoftware.os.amza.shared.wal.WALRow;
import com.jivesoftware.os.amza.shared.wal.WALTimestampId;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
                            WALRow row = primaryRowMarshaller.fromRow(rawRow);
                            ByteBuffer bb = ByteBuffer.wrap(row.key.getKey());
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
                                    WALKey key = new WALKey(keyBytes);
                                    WALValue partitionValue = partitionStore.get(key);
                                    if (partitionValue == null || partitionValue.getTimestampId() < row.value.getTimestampId()) {
                                        WALPointer got = delta.getPointer(key);
                                        if (got == null || got.getTimestampId() < row.value.getTimestampId()) {
                                            delta.put(rowFP, wal.hydrateKeyValueHighwater(row));
                                            //TODO this makes the txId partially visible to takes, need to prevent operations until fully loaded
                                            delta.appendTxFps(rowTxId, rowFP);
                                        }
                                    }
                                } finally {
                                    releaseOne();
                                }
                            }
                            updateSinceLastCompaction.incrementAndGet();
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
        Commitable<WALValue> updates,
        WALUpdated updated) throws Exception {

        final AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);

        final Table<Long, WALKey, WALValue> apply = Tables.newCustomTable(new LinkedHashMap<>(), LinkedHashMap::new);

        final Map<WALKey, WALTimestampId> removes = new HashMap<>();
        final Map<WALKey, WALTimestampId> clobbers = new HashMap<>();

        final List<WALKey> keys = new ArrayList<>();
        final List<WALValue> values = new ArrayList<>();
        updates.commitable(null, (long transactionId, WALKey key, WALValue update) -> {
            keys.add(key);
            values.add(update);
            return true;
        });

        acquireOne();
        try {
            DeltaWAL wal = deltaWAL.get();
            RowsChanged rowsChanged;

            getTimestamps(versionedPartitionName, storage, (KeyValueStream stream) -> {
                for (int i = 0; i < keys.size(); i++) {
                    WALKey key = keys.get(i);
                    WALValue update = values.get(i);
                    stream.stream(key, update);
                }
            }, (WALKey key, WALValue value, long currentTimestamp, boolean currentTombstoned, long currentFp) -> {

                if (currentFp == -1) {
                    apply.put(-1L, key, value);
                    if (oldestAppliedTimestamp.get() > value.getTimestampId()) {
                        oldestAppliedTimestamp.set(value.getTimestampId());
                    }
                } else if (currentTimestamp < value.getTimestampId()) {
                    apply.put(-1L, key, value);
                    if (oldestAppliedTimestamp.get() > value.getTimestampId()) {
                        oldestAppliedTimestamp.set(value.getTimestampId());
                    }
                    WALTimestampId walTimestampId = new WALTimestampId(currentTimestamp, currentTombstoned);
                    clobbers.put(key, walTimestampId);
                    if (value.getTombstoned() && !currentTombstoned) {
                        removes.put(key, walTimestampId);
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
                        WALKey key = keyValueHighwater.key;
                        WALValue value = keyValueHighwater.value;

                        WALPointer got = delta.getPointer(key);
                        if (got == null || got.getTimestampId() < value.getTimestampId()) {
                            delta.put(fp, keyValueHighwater);
                        } else {
                            apply.remove(-1L, key);
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

    public WALValue get(VersionedPartitionName versionedPartitionName, WALStorage storage, WALKey key) throws Exception {
        acquireOne();
        try {
            Optional<WALValue> deltaGot = getPartitionDelta(versionedPartitionName).get(key);
            if (deltaGot != null) {
                return deltaGot.orNull();
            }
        } finally {
            releaseOne();
        }
        return storage.get(key);

    }

    public void get(VersionedPartitionName versionedPartitionName, WALStorage storage, KeyValues keyValues, TimestampKeyValueStream stream) throws Exception {
        acquireOne();
        try {
            PartitionDelta partitionDelta = getPartitionDelta(versionedPartitionName);
            storage.get((KeyValueStream storageStream) -> {

                partitionDelta.get(keyValues, (WALKey key, WALValue value) -> {
                    if (value == null) {
                        return storageStream.stream(key, null);
                    } else {
                        if (value.getTombstoned()) {
                            return stream.stream(key, null, -1);
                        } else {
                            return stream.stream(key, value.getValue(), value.getTimestampId());
                        }
                    }
                });

            }, (WALKey key, WALValue value) -> {
                if (value.getTombstoned()) {
                    return stream.stream(key, null, -1);
                } else {
                    return stream.stream(key, value.getValue(), value.getTimestampId());
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

    private void getTimestamps(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        KeyValues keyValues,
        WALKeyValuePointerStream stream) throws Exception {

        storage.streamWALPointers((KeyValueStream storageStream) -> {
            PartitionDelta partitionDelta = getPartitionDelta(versionedPartitionName);
            partitionDelta.getPointers(keyValues, (WALKey key, WALValue value, long timestamp, boolean tombstoned, long fp) -> {
                if (fp == -1) {
                    storageStream.stream(key, value);
                } else {
                    stream.stream(key, value, timestamp, tombstoned, fp);
                }
                return true;
            });
        }, (WALKey key, WALValue value, long timestamp, boolean tombstoned, long fp) -> {
            if (fp == -1) {
                stream.stream(key, value, -1, false, -1);
            } else {
                stream.stream(key, value, timestamp, tombstoned, fp);
            }
            return true;
        });

    }

    public void rangeScan(final VersionedPartitionName versionedPartitionName, RangeScannable<WALValue> rangeScannable, WALKey from, WALKey to,
        final Scan<WALValue> scan)
        throws Exception {
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            final DeltaPeekableElmoIterator iterator = delta.rangeScanIterator(from, to);
            rangeScannable.rangeScan(from, to, new Dupinator(iterator, scan));

            Map.Entry<WALKey, WALValue> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    WALValue got = d.getValue();
                    scan.row(-1, d.getKey(), got);
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    WALValue got = d.getValue();
                    if (!scan.row(-1, d.getKey(), got)) {
                        return;
                    }
                }
            }
        } finally {
            releaseOne();
        }
    }

    public void rowScan(final VersionedPartitionName versionedPartitionName, Scannable<WALValue> scanable, final Scan<WALValue> scan) throws Exception {
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            final DeltaPeekableElmoIterator iterator = delta.rowScanIterator();
            scanable.rowScan(new Dupinator(iterator, scan));

            Map.Entry<WALKey, WALValue> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    WALValue got = d.getValue();
                    scan.row(-1, d.getKey(), got);
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    WALValue got = d.getValue();
                    if (!scan.row(-1, d.getKey(), got)) {
                        return;
                    }
                }
            }
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

    static class Dupinator implements Scan<WALValue> {

        private final DeltaPeekableElmoIterator iterator;
        private final Scan<WALValue> scan;
        private Map.Entry<WALKey, WALValue> d;

        public Dupinator(DeltaPeekableElmoIterator iterator, Scan<WALValue> scan) {
            this.iterator = iterator;
            this.scan = scan;
        }

        @Override
        public boolean row(long rowTxId, WALKey key, WALValue value) throws Exception {
            if (d == null && iterator.hasNext()) {
                d = iterator.next();
            }
            boolean needsKey = true;
            while (d != null && d.getKey().compareTo(key) <= 0) {
                WALValue got = d.getValue();
                if (d.getKey().equals(key)) {
                    needsKey = false;
                }
                if (!scan.row(-1, d.getKey(), got)) {
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
                return scan.row(-1, key, value);
            } else {
                return true;
            }
        }
    }

}
