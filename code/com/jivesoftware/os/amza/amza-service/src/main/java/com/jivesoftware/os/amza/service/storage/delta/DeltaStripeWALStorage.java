package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALTimestampId;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.WALRow;
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
public class DeltaStripeWALStorage implements StripeWALStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config

    private final int index;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final DeltaWALFactory deltaWALFactory;
    private final AtomicReference<DeltaWAL> deltaWAL = new AtomicReference<>();
    private final long compactAfterNUpdates;
    private final ConcurrentHashMap<RegionName, RegionDelta> regionDeltas = new ConcurrentHashMap<>();
    private final Object oneWriterAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final ExecutorService compactionThreads;
    private final AtomicLong updateSinceLastCompaction = new AtomicLong();
    private final AtomicBoolean compacting = new AtomicBoolean(false);

    private final ThreadLocal<Integer> reentrant = new ThreadLocal<Integer>() {

        @Override
        protected Integer initialValue() {
            return 0;
        }
    };

    public DeltaStripeWALStorage(int index,
        RowMarshaller<byte[]> rowMarshaller,
        DeltaWALFactory deltaWALFactory,
        long compactAfterNUpdates) {

        this.index = index;
        this.rowMarshaller = rowMarshaller;
        this.deltaWALFactory = deltaWALFactory;
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

    @Override
    public void load(final RegionIndex regionIndex) throws Exception {
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
                        compactDelta(regionIndex, deltaWAL.get(), new Callable<DeltaWAL>() {

                            @Override
                            public DeltaWAL call() throws Exception {
                                return wal;
                            }
                        });
                    }
                    deltaWAL.set(wal);
                    wal.load(new RowStream() {

                        @Override
                        public boolean row(long rowFP, final long rowTxId, byte rowType, byte[] rawRow) throws Exception {
                            if (rowType > 0) {
                                WALRow row = rowMarshaller.fromRow(rawRow);
                                WALValue value = row.getValue();
                                ByteBuffer bb = ByteBuffer.wrap(row.getKey().getKey());
                                byte[] regionNameBytes = new byte[bb.getShort()];
                                bb.get(regionNameBytes);
                                final byte[] keyBytes = new byte[bb.getInt()];
                                bb.get(keyBytes);

                                RegionName regionName = RegionName.fromBytes(regionNameBytes);
                                RegionStore regionStore = regionIndex.get(regionName);
                                if (regionStore == null) {
                                    LOG.error("Should be impossible must fix! Your it :) regionName:" + regionName);
                                } else {
                                    acquireOne();
                                    try {
                                        RegionDelta delta = getRegionDeltas(regionName);
                                        WALKey key = new WALKey(keyBytes);
                                        WALValue regionValue = regionStore.get(key);
                                        if (regionValue == null || regionValue.getTimestampId() < value.getTimestampId()) {
                                            WALTimestampId got = delta.getTimestampId(key);
                                            if (got == null || got.getTimestampId() < value.getTimestampId()) {
                                                delta.put(key, new WALPointer(rowFP, value.getTimestampId(), value.getTombstoned()));
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
                        }
                    });

                }
            }
        }
        LOG.info("Reloaded deltas stripe:{} in {} ms", index, (System.currentTimeMillis() - start));
    }

    @Override
    public void flush(boolean fsync) throws Exception {
        DeltaWAL wal = deltaWAL.get();
        if (wal != null) {
            wal.flush(fsync);
        }
    }

    // todo any one call this should have atleast 1 numTickleMeElmaphore
    private RegionDelta getRegionDeltas(RegionName regionName) {
        RegionDelta regionDelta = regionDeltas.get(regionName);
        if (regionDelta == null) {
            DeltaWAL wal = deltaWAL.get();
            if (wal == null) {
                throw new IllegalStateException("Delta WAL is currently unavailable.");
            }
            regionDelta = new RegionDelta(regionName, wal, rowMarshaller, null);
            RegionDelta had = regionDeltas.putIfAbsent(regionName, regionDelta);
            if (had != null) {
                regionDelta = had;
            }
        }
        return regionDelta;
    }

    @Override
    public void compact(final RegionIndex regionIndex) throws Exception {
        if (updateSinceLastCompaction.get() < compactAfterNUpdates) { // TODO or some memory pressure BS!
            return;
        }

        if (!compacting.compareAndSet(false, true)) {
            LOG.warn("Trying to compact DeltaStripe:" + regionIndex + " while another compaction is already in progress.");
            return;
        }
        try {
            updateSinceLastCompaction.set(0);
            compactDelta(regionIndex, deltaWAL.get(), new Callable<DeltaWAL>() {

                @Override
                public DeltaWAL call() throws Exception {
                    return deltaWALFactory.create();
                }
            });
        } finally {
            compacting.set(false);
        }
    }

    private void compactDelta(final RegionIndex regionIndex, DeltaWAL wal, Callable<DeltaWAL> newWAL) throws Exception {
        final List<Future<Boolean>> futures = new ArrayList<>();
        acquireAll();
        try {
            for (Map.Entry<RegionName, RegionDelta> e : regionDeltas.entrySet()) {
                if (e.getValue().compacting.get() != null) {
                    LOG.warn("Ingress is faster than we can compact!");
                    return;
                }
            }
            LOG.info("Compacting delta regions...");
            DeltaWAL newDeltaWAL = newWAL.call();
            deltaWAL.set(newDeltaWAL);
            for (Map.Entry<RegionName, RegionDelta> e : regionDeltas.entrySet()) {
                final RegionDelta regionDelta = new RegionDelta(e.getKey(), newDeltaWAL, rowMarshaller, e.getValue());
                regionDeltas.put(e.getKey(), regionDelta);
                futures.add(compactionThreads.submit(new Callable<Boolean>() {

                    @Override
                    public Boolean call() throws Exception {
                        try {
                            regionDelta.compact(regionIndex);
                            return true;
                        } catch (Exception x) {
                            LOG.error("Failed to compact:" + regionDelta, x);
                            return false;
                        }
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
                LOG.info("Compacted delta regions.");
            } else {
                LOG.warn("Compaction of delta region FAILED.");
            }
        } finally {
            releaseAll();
        }
    }

    @Override
    public RowsChanged update(RegionName regionName,
        WALStorage storage,
        WALReplicator replicator,
        WALStorageUpdateMode mode,
        Scannable<WALValue> updates) throws Exception {

        final AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);
        final Table<Long, WALKey, WALValue> apply = TreeBasedTable.create();
        final Map<WALKey, WALTimestampId> removes = new HashMap<>();
        final Map<WALKey, WALTimestampId> clobbers = new HashMap<>();

        final List<WALKey> keys = new ArrayList<>();
        final List<WALValue> values = new ArrayList<>();
        updates.rowScan(new Scan<WALValue>() {
            @Override
            public boolean row(long transactionId, WALKey key, WALValue update) throws Exception {
                keys.add(key);
                values.add(update);
                return true;
            }
        });

        acquireOne();
        try {
            DeltaWAL wal = deltaWAL.get();
            RowsChanged rowsChanged;

            // only grabbing pointers means our removes and clobbers don't include the old values, but for now this is more efficient.
            WALTimestampId[] currentTimestamps = getTimestamps(regionName, storage, keys, values);
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

            List<Future<?>> futures = Lists.newArrayListWithCapacity(1);
            if (replicator != null && mode == WALStorageUpdateMode.replicateThenUpdate && !apply.isEmpty()) {
                futures.add(replicator.replicate(new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers)));
            }

            if (apply.isEmpty()) {
                rowsChanged = new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
            } else {

                RegionDelta delta = getRegionDeltas(regionName);
                synchronized (oneWriterAtATimeLock) {
                    DeltaWAL.DeltaWALApplied updateApplied = wal.update(regionName, apply);

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
                    rowsChanged = new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
                }

                if (replicator != null && mode == WALStorageUpdateMode.updateThenReplicate && !apply.isEmpty()) {
                    futures.add(replicator.replicate(new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers)));
                }
            }
            for (Future<?> future : futures) {
                future.get();
            }

            updateSinceLastCompaction.addAndGet(apply.size());
            return rowsChanged;
        } finally {
            releaseOne();
        }
    }

    @Override
    public void takeRowUpdatesSince(RegionName regionName, WALStorage storage, long transactionId, final RowStream rowStream) throws Exception {
        if (!storage.takeRowUpdatesSince(transactionId, rowStream)) {
            return;
        }

        acquireOne();
        try {
            RegionDelta delta = getRegionDeltas(regionName);
            delta.takeRowUpdatesSince(transactionId, rowStream);
        } finally {
            releaseOne();
        }
    }

    @Override
    public boolean takeFromTransactionId(RegionName regionName, WALStorage storage, long transactionId, Scan<WALValue> scan) throws Exception {
        if (!storage.takeFromTransactionId(transactionId, scan)) {
            return false;
        }

        acquireOne();
        try {
            RegionDelta delta = getRegionDeltas(regionName);
            return delta.takeFromTransactionId(transactionId, scan);
        } finally {
            releaseOne();
        }
    }

    @Override
    public WALValue get(RegionName regionName, WALStorage storage, WALKey key) throws Exception {
        WALValue got;
        acquireOne();
        try {
            got = getRegionDeltas(regionName).get(key);
        } finally {
            releaseOne();
        }
        if (got == null) {
            got = storage.get(key);
        }
        return got;

    }

    @Override
    public boolean containsKey(RegionName regionName, WALStorage storage, WALKey key) throws Exception {
        boolean contains;
        acquireOne();
        try {
            contains = getRegionDeltas(regionName).containsKey(key);
        } finally {
            releaseOne();
        }
        return contains || storage.containsKey(key);
    }

    private WALTimestampId[] getTimestamps(RegionName regionName, WALStorage storage, List<WALKey> keys, List<WALValue> values) throws Exception {
        WALKey[] consumableKeys = keys.toArray(new WALKey[keys.size()]);
        DeltaResult<WALTimestampId[]> deltas = getRegionDeltas(regionName).getTimestampIds(consumableKeys);
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

    @Override
    public void rangeScan(final RegionName regionName, RangeScannable<WALValue> rangeScannable, WALKey from, WALKey to, final Scan<WALValue> scan)
        throws Exception {
        acquireOne();
        try {
            RegionDelta delta = getRegionDeltas(regionName);
            final DeltaPeekableElmoIterator iterator = delta.rangeScanIterator(from, to);
            rangeScannable.rangeScan(from, to, new Scan<WALValue>() {
                Map.Entry<WALKey, WALValue> d;

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
            });

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

    @Override
    public void rowScan(final RegionName regionName, Scannable<WALValue> scanable, final Scan<WALValue> scan) throws Exception {
        acquireOne();
        try {
            RegionDelta delta = getRegionDeltas(regionName);
            final DeltaPeekableElmoIterator iterator = delta.rowScanIterator();
            scanable.rowScan(new Scan<WALValue>() {
                Map.Entry<WALKey, WALValue> d;

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
            });

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
     *
     * @param regionName
     * @param storage
     * @return
     * @throws Exception
     */
    @Override
    public long count(RegionName regionName, WALStorage storage) throws Exception {
        int count = 0;
        acquireOne();
        try {
            ArrayList<WALKey> keys = new ArrayList<>(getRegionDeltas(regionName).keySet());
            List<Boolean> containsKey = storage.containsKey(keys);
            count = Iterables.frequency(containsKey, Boolean.FALSE);
        } finally {
            releaseOne();
        }
        return count + storage.count();
    }

}
