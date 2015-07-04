package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
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
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
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
import java.util.Iterator;
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
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final DeltaWALFactory deltaWALFactory;
    private final AtomicReference<DeltaWAL> deltaWAL = new AtomicReference<>();
    private final WALUpdated walUpdated;
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
        PrimaryRowMarshaller<byte[]> primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        DeltaWALFactory deltaWALFactory,
        WALUpdated walUpdated,
        long compactAfterNUpdates) {

        this.index = index;
        this.amzaStats = amzaStats;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.deltaWALFactory = deltaWALFactory;
        this.walUpdated = walUpdated;
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

    public boolean expunge(VersionedPartitionName versionedPartitionName, WALStorage walStorage) throws Exception {
        acquireAll();
        try {
            partitionDeltas.remove(versionedPartitionName);
            return true;
        } finally {
            releaseAll();
        }
    }

    public void load(final PartitionIndex partitionIndex) throws Exception {
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
                                    PartitionDelta delta = getPartitionDeltas(versionedPartitionName);
                                    WALKey key = new WALKey(keyBytes);
                                    WALValue partitionValue = partitionStore.get(key);
                                    if (partitionValue == null || partitionValue.getTimestampId() < row.value.getTimestampId()) {
                                        WALTimestampId got = delta.getTimestampId(key);
                                        if (got == null || got.getTimestampId() < row.value.getTimestampId()) {
                                            delta.put(key, new WALPointer(rowFP, row.value.getTimestampId(), row.value.getTombstoned()));
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
    private PartitionDelta getPartitionDeltas(VersionedPartitionName versionedPartitionName) {
        PartitionDelta partitionDelta = partitionDeltas.get(versionedPartitionName);
        if (partitionDelta == null) {
            DeltaWAL wal = deltaWAL.get();
            if (wal == null) {
                throw new IllegalStateException("Delta WAL is currently unavailable.");
            }
            partitionDelta = new PartitionDelta(versionedPartitionName, wal, walUpdated, primaryRowMarshaller, highwaterRowMarshaller, null);
            PartitionDelta had = partitionDeltas.putIfAbsent(versionedPartitionName, partitionDelta);
            if (had != null) {
                partitionDelta = had;
            }
        }
        return partitionDelta;
    }

    public void compact(PartitionIndex partitionIndex, boolean force) throws Exception {
        if (!force && updateSinceLastCompaction.get() < compactAfterNUpdates) { // TODO or some memory pressure BS!
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
                    walUpdated,
                    primaryRowMarshaller,
                    highwaterRowMarshaller,
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
        final Table<Long, WALKey, WALValue> apply = TreeBasedTable.create();
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

            // only grabbing pointers means our removes and clobbers don't include the old values, but for now this is more efficient.
            WALTimestampId[] currentTimestamps = getTimestamps(versionedPartitionName, storage, keys, values);
            for (int i = 0; i < keys.size(); i++) {
                WALKey key = keys.get(i);
                WALTimestampId currentTimestamp = currentTimestamps[i];
                WALValue update = values.get(i);
                if (currentTimestamp == null) {
                    apply.put(-1L, key, update);
                    if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                        oldestAppliedTimestamp.set(update.getTimestampId());
                    }
                } else if (currentTimestamp.getTimestampId() < update.getTimestampId()) {
                    apply.put(-1L, key, update);
                    if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                        oldestAppliedTimestamp.set(update.getTimestampId());
                    }
                    clobbers.put(key, currentTimestamp);
                    if (update.getTombstoned() && !currentTimestamp.getTombstoned()) {
                        removes.put(key, currentTimestamp);
                    }
                }
            }

            if (apply.isEmpty()) {
                rowsChanged = new RowsChanged(versionedPartitionName, oldestAppliedTimestamp.get(), apply, removes, clobbers, -1);
            } else {
                PartitionDelta delta = getPartitionDeltas(versionedPartitionName);
                WALHighwater partitionHighwater = null;
                if (delta.shouldWriteHighwater()) {
                    partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
                    LOG.inc("highwaterHint", 1);
                    LOG.inc("highwaterHint", 1, versionedPartitionName.getPartitionName().getName());
                }
                DeltaWAL.DeltaWALApplied updateApplied;
                synchronized (oneWriterAtATimeLock) {
                    updateApplied = wal.update(versionedPartitionName, apply, partitionHighwater);

                    Iterator<Table.Cell<Long, WALKey, WALValue>> iter = apply.cellSet().iterator();
                    while (iter.hasNext()) {
                        Table.Cell<Long, WALKey, WALValue> cell = iter.next();
                        WALKey key = cell.getColumnKey();
                        WALValue value = cell.getValue();
                        long pointer = updateApplied.keyToRowPointer.get(key);
                        WALPointer rowPointer = new WALPointer(pointer, value.getTimestampId(), value.getTombstoned());

                        WALTimestampId got = delta.getTimestampId(key);
                        if (got == null || got.getTimestampId() < value.getTimestampId()) {
                            delta.put(key, rowPointer);
                        } else {
                            iter.remove();
                        }
                    }
                    delta.appendTxFps(updateApplied.txId, updateApplied.keyToRowPointer.values());
                    rowsChanged = new RowsChanged(versionedPartitionName, oldestAppliedTimestamp.get(), apply, removes, clobbers, updateApplied.txId);
                }
                updated.updated(versionedPartitionName, partitionStatus, updateApplied.txId);
            }

            updateSinceLastCompaction.addAndGet(apply.size());
            return rowsChanged;
        } finally {
            releaseOne();
        }
    }

    public void takeRowUpdatesSince(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        long transactionId,
        RowStream rowStream) throws Exception {
        if (!storage.takeRowUpdatesSince(transactionId, rowStream)) {
            return;
        }

        acquireOne();
        try {
            PartitionDelta delta = getPartitionDeltas(versionedPartitionName);
            delta.takeRowUpdatesSince(transactionId, rowStream);
        } finally {
            releaseOne();
        }
    }

    public boolean takeFromTransactionId(VersionedPartitionName versionedPartitionName, WALStorage storage, long transactionId, Highwaters highwaters,
        Scan<WALValue> scan)
        throws Exception {

        if (!storage.takeFromTransactionId(transactionId, highwaters, scan)) {
            return false;
        }

        acquireOne();
        try {
            PartitionDelta delta = getPartitionDeltas(versionedPartitionName);
            return delta.takeFromTransactionId(transactionId, highwaters, scan);
        } finally {
            releaseOne();
        }
    }

    public WALValue get(VersionedPartitionName versionedPartitionName, WALStorage storage, WALKey key) throws Exception {
        acquireOne();
        try {
            Optional<WALValue> deltaGot = getPartitionDeltas(versionedPartitionName).get(key);
            if (deltaGot != null) {
                return deltaGot.orNull();
            }
        } finally {
            releaseOne();
        }
        return storage.get(key);

    }

    public boolean containsKey(VersionedPartitionName versionedPartitionName, WALStorage storage, WALKey key) throws Exception {
        acquireOne();
        try {
            Boolean contained = getPartitionDeltas(versionedPartitionName).containsKey(key);
            if (contained != null) {
                return contained;
            }
        } finally {
            releaseOne();
        }
        return storage.containsKey(key);
    }

    private WALTimestampId[] getTimestamps(VersionedPartitionName versionedPartitionName, WALStorage storage, List<WALKey> keys, List<WALValue> values) throws
        Exception {
        WALKey[] consumableKeys = keys.toArray(new WALKey[keys.size()]);
        DeltaResult<WALTimestampId[]> deltas = getPartitionDeltas(versionedPartitionName).getTimestampIds(consumableKeys);
        if (deltas.missed) {
            WALTimestampId[] timestamps = deltas.result;
            WALPointer[] got = storage.getPointers(consumableKeys, values);
            for (int i = 0; i < timestamps.length; i++) {
                if (timestamps[i] == null && got[i] != null) {
                    timestamps[i] = new WALTimestampId(got[i].getTimestampId(), got[i].getTombstoned());
                }
            }
            return timestamps;
        } else {
            return deltas.result;
        }
    }

    public void rangeScan(final VersionedPartitionName versionedPartitionName, RangeScannable<WALValue> rangeScannable, WALKey from, WALKey to,
        final Scan<WALValue> scan)
        throws Exception {
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDeltas(versionedPartitionName);
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
            PartitionDelta delta = getPartitionDeltas(versionedPartitionName);
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
            ArrayList<WALKey> keys = new ArrayList<>(getPartitionDeltas(versionedPartitionName).keySet());
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
