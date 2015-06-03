package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.Commitable;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.Highwaters;
import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowType;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALHighwater;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALTimestampId;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.PrimaryRowMarshaller;
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

    private final HighwaterStorage highwaterStorage;
    private final int index;
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final DeltaWALFactory deltaWALFactory;
    private final AtomicReference<DeltaWAL> deltaWAL = new AtomicReference<>();
    private final long compactAfterNUpdates;
    private final ConcurrentHashMap<VersionedRegionName, RegionDelta> regionDeltas = new ConcurrentHashMap<>();
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

    public DeltaStripeWALStorage(HighwaterStorage highwaterStorage,
        int index,
        PrimaryRowMarshaller<byte[]> primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        DeltaWALFactory deltaWALFactory,
        long compactAfterNUpdates) {

        this.highwaterStorage = highwaterStorage;
        this.index = index;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
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
    public boolean expunge(VersionedRegionName versionedRegionName, WALStorage walStorage) throws Exception {
        acquireAll();
        boolean expunged = true;
        try {
            expunged &= highwaterStorage.expunge(versionedRegionName);
            regionDeltas.remove(versionedRegionName);
            return expunged;
        } finally {
            releaseAll();
        }
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
                        compactDelta(regionIndex, deltaWAL.get(), () -> wal);
                    }
                    deltaWAL.set(wal);
                    wal.load((long rowFP, final long rowTxId, RowType rowType, byte[] rawRow) -> {
                        if (rowType == RowType.primary) {
                            WALRow row = primaryRowMarshaller.fromRow(rawRow);
                            ByteBuffer bb = ByteBuffer.wrap(row.key.getKey());
                            byte[] regionNameBytes = new byte[bb.getShort()];
                            bb.get(regionNameBytes);
                            final byte[] keyBytes = new byte[bb.getInt()];
                            bb.get(keyBytes);

                            VersionedRegionName versionedRegionName = VersionedRegionName.fromBytes(regionNameBytes);
                            RegionStore regionStore = regionIndex.get(versionedRegionName);
                            if (regionStore == null) {
                                LOG.error("Should be impossible must fix! Your it :) regionName:" + versionedRegionName);
                            } else {
                                acquireOne();
                                try {
                                    RegionDelta delta = getRegionDeltas(versionedRegionName);
                                    WALKey key = new WALKey(keyBytes);
                                    WALValue regionValue = regionStore.get(key);
                                    if (regionValue == null || regionValue.getTimestampId() < row.value.getTimestampId()) {
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

    @Override
    public void flush(boolean fsync) throws Exception {
        DeltaWAL wal = deltaWAL.get();
        if (wal != null) {
            wal.flush(fsync);
        }
    }

    // todo any one call this should have atleast 1 numTickleMeElmaphore
    private RegionDelta getRegionDeltas(VersionedRegionName versionedRegionName) {
        RegionDelta regionDelta = regionDeltas.get(versionedRegionName);
        if (regionDelta == null) {
            DeltaWAL wal = deltaWAL.get();
            if (wal == null) {
                throw new IllegalStateException("Delta WAL is currently unavailable.");
            }
            regionDelta = new RegionDelta(versionedRegionName, wal, primaryRowMarshaller, highwaterRowMarshaller, null);
            RegionDelta had = regionDeltas.putIfAbsent(versionedRegionName, regionDelta);
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
            compactDelta(regionIndex, deltaWAL.get(), deltaWALFactory::create);
        } finally {
            compacting.set(false);
        }
    }

    private void compactDelta(final RegionIndex regionIndex, DeltaWAL wal, Callable<DeltaWAL> newWAL) throws Exception {
        final List<Future<Boolean>> futures = new ArrayList<>();
        acquireAll();
        try {
            for (Map.Entry<VersionedRegionName, RegionDelta> e : regionDeltas.entrySet()) {
                if (e.getValue().compacting.get() != null) {
                    LOG.warn("Ingress is faster than we can compact!");
                    return;
                }
            }
            LOG.info("Compacting delta regions...");
            DeltaWAL newDeltaWAL = newWAL.call();
            deltaWAL.set(newDeltaWAL);
            for (Map.Entry<VersionedRegionName, RegionDelta> e : regionDeltas.entrySet()) {
                final RegionDelta regionDelta = new RegionDelta(e.getKey(), newDeltaWAL, primaryRowMarshaller, highwaterRowMarshaller, e.getValue());
                regionDeltas.put(e.getKey(), regionDelta);
                futures.add(compactionThreads.submit(() -> {
                    try {
                        regionDelta.compact(regionIndex);
                        return true;
                    } catch (Exception x) {
                        LOG.error("Failed to compact:" + regionDelta, x);
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
                LOG.info("Compacted delta regions.");
            } else {
                LOG.warn("Compaction of delta region FAILED.");
            }
        } finally {
            releaseAll();
        }
    }

    @Override
    public RowsChanged update(VersionedRegionName versionedRegionName,
        WALStorage storage,
        Commitable<WALValue> updates) throws Exception {

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
            WALTimestampId[] currentTimestamps = getTimestamps(versionedRegionName, storage, keys, values);
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
                rowsChanged = new RowsChanged(versionedRegionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
            } else {
                RegionDelta delta = getRegionDeltas(versionedRegionName);
                WALHighwater regionHighwater = null;
                if (delta.shouldWriteHighwater()) {
                    regionHighwater = highwaterStorage.getRegionHighwater(versionedRegionName);
                    LOG.inc("highwaterHint", 1);
                    LOG.inc("highwaterHint", 1, versionedRegionName.getRegionName().getRegionName());
                }

                synchronized (oneWriterAtATimeLock) {
                    DeltaWAL.DeltaWALApplied updateApplied = wal.update(versionedRegionName, apply, regionHighwater);

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
                    rowsChanged = new RowsChanged(versionedRegionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
                }
            }

            updateSinceLastCompaction.addAndGet(apply.size());
            return rowsChanged;
        } finally {
            releaseOne();
        }
    }

    @Override
    public void takeRowUpdatesSince(VersionedRegionName versionedRegionName, WALStorage storage, long transactionId, RowStream rowStream) throws Exception {
        if (!storage.takeRowUpdatesSince(transactionId, rowStream)) {
            return;
        }

        acquireOne();
        try {
            RegionDelta delta = getRegionDeltas(versionedRegionName);
            delta.takeRowUpdatesSince(transactionId, rowStream);
        } finally {
            releaseOne();
        }
    }

    @Override
    public boolean takeFromTransactionId(VersionedRegionName versionedRegionName, WALStorage storage, long transactionId, Highwaters highwaters,
        Scan<WALValue> scan)
        throws Exception {

        if (!storage.takeFromTransactionId(transactionId, highwaters, scan)) {
            return false;
        }

        acquireOne();
        try {
            RegionDelta delta = getRegionDeltas(versionedRegionName);
            return delta.takeFromTransactionId(transactionId, highwaters, scan);
        } finally {
            releaseOne();
        }
    }

    @Override
    public WALValue get(VersionedRegionName versionedRegionName, WALStorage storage, WALKey key) throws Exception {
        acquireOne();
        try {
            Optional<WALValue> deltaGot = getRegionDeltas(versionedRegionName).get(key);
            if (deltaGot != null) {
                return deltaGot.orNull();
            }
        } finally {
            releaseOne();
        }
        return storage.get(key);

    }

    @Override
    public boolean containsKey(VersionedRegionName versionedRegionName, WALStorage storage, WALKey key) throws Exception {
        acquireOne();
        try {
            Boolean contained = getRegionDeltas(versionedRegionName).containsKey(key);
            if (contained != null) {
                return contained;
            }
        } finally {
            releaseOne();
        }
        return storage.containsKey(key);
    }

    private WALTimestampId[] getTimestamps(VersionedRegionName versionedRegionName, WALStorage storage, List<WALKey> keys, List<WALValue> values) throws
        Exception {
        WALKey[] consumableKeys = keys.toArray(new WALKey[keys.size()]);
        DeltaResult<WALTimestampId[]> deltas = getRegionDeltas(versionedRegionName).getTimestampIds(consumableKeys);
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
    public void rangeScan(final VersionedRegionName versionedRegionName, RangeScannable<WALValue> rangeScannable, WALKey from, WALKey to,
        final Scan<WALValue> scan)
        throws Exception {
        acquireOne();
        try {
            RegionDelta delta = getRegionDeltas(versionedRegionName);
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
    public void rowScan(final VersionedRegionName versionedRegionName, Scannable<WALValue> scanable, final Scan<WALValue> scan) throws Exception {
        acquireOne();
        try {
            RegionDelta delta = getRegionDeltas(versionedRegionName);
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
     * @param regionName RegionName
     * @param storage Storage
     * @return long
     * @throws Exception .
     */
    @Override
    public long count(VersionedRegionName versionedRegionName, WALStorage storage) throws Exception {
        int count = 0;
        acquireOne();
        try {
            ArrayList<WALKey> keys = new ArrayList<>(getRegionDeltas(versionedRegionName).keySet());
            List<Boolean> containsKey = storage.containsKey(keys);
            count = Iterables.frequency(containsKey, Boolean.FALSE);
        } finally {
            releaseOne();
        }
        return count + storage.count();
    }

}
