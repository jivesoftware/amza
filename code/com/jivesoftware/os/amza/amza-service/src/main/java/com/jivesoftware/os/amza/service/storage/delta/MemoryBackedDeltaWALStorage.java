package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
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
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class MemoryBackedDeltaWALStorage implements DeltaWALStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config

    private final RowMarshaller<byte[]> rowMarshaller;
    private final DeltaWAL deltaWAL;
    private final WALReplicator walReplicator;
    private final long compactAfterNUpdates;
    private final ConcurrentHashMap<RegionName, RegionDelta> regionDeltas = new ConcurrentHashMap<>();
    private final Object oneWriterAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final ExecutorService compactionThreads;
    private final AtomicLong updateSinceLastCompaction = new AtomicLong();

    public MemoryBackedDeltaWALStorage(int index,
        RowMarshaller<byte[]> rowMarshaller,
        DeltaWAL deltaWAL,
        WALReplicator walReplicator,
        long compactAfterNUpdates) {

        this.rowMarshaller = rowMarshaller;
        this.deltaWAL = deltaWAL;
        this.walReplicator = walReplicator;
        this.compactAfterNUpdates = compactAfterNUpdates;
        this.compactionThreads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setNameFormat("compact-deltas-" + index + "-%d").build()); // TODO pass in
    }

    @Override
    public void load(final RegionProvider regionProvider) throws Exception {
        LOG.info("Reloading deltas...");
        long start = System.currentTimeMillis();
        synchronized (oneWriterAtATimeLock) {
            deltaWAL.load(new RowStream() {

                @Override
                public boolean row(long rowFP, final long rowTxId, byte rowType, byte[] rawRow) throws Exception {
                    RowMarshaller.WALRow row = rowMarshaller.fromRow(rawRow);
                    WALValue value = row.getValue();
                    ByteBuffer bb = ByteBuffer.wrap(row.getKey().getKey());
                    byte[] regionNameBytes = new byte[bb.getShort()];
                    bb.get(regionNameBytes);
                    final byte[] keyBytes = new byte[bb.getInt()];
                    bb.get(keyBytes);

                    RegionName regionName = RegionName.fromBytes(regionNameBytes);
                    RegionStore regionStore = regionProvider.getRegionStore(regionName);
                    if (regionStore == null) {
                        LOG.error("Should be impossible must fix! Your it :) regionName:" + regionName);
                    } else {
                        tickleMeElmophore.acquire();
                        try {
                            RegionDelta delta = getRegionDeltas(regionName);
                            WALKey key = new WALKey(keyBytes);
                            WALValue regionValue = regionStore.get(key);
                            if (regionValue == null || regionValue.getTimestampId() < value.getTimestampId()) {
                                WALPointer got = delta.getPointer(key);
                                if (got == null || got.getTimestampId() < value.getTimestampId()) {
                                    delta.put(key, new WALPointer(rowFP, value.getTimestampId(), value.getTombstoned()));
                                }
                            }
                            //TODO this makes the txId partially visible to takes, need to prevent operations until fully loaded
                            delta.appendTxFps(rowTxId, rowFP);
                        } finally {
                            tickleMeElmophore.release();
                        }
                    }
                    updateSinceLastCompaction.incrementAndGet();
                    return true;
                }
            });
        }
        LOG.info("Reloaded deltas in {} ms", (System.currentTimeMillis() - start));
    }

    // todo any one call this should have atleast 1 numTickleMeElmaphore
    private RegionDelta getRegionDeltas(RegionName regionName) {
        RegionDelta regionDelta = regionDeltas.get(regionName);
        if (regionDelta == null) {
            regionDelta = new RegionDelta(regionName, deltaWAL, null);
            RegionDelta had = regionDeltas.putIfAbsent(regionName, regionDelta);
            if (had != null) {
                regionDelta = had;
            }
        }
        return regionDelta;
    }

    @Override
    public void compact(final RegionProvider regionProvider) throws Exception {
        if (updateSinceLastCompaction.get() < compactAfterNUpdates) { // TODO or some memory pressure BS!
            return;
        }
        updateSinceLastCompaction.set(0);

        final List<Future<Long>> futures = new ArrayList<>();
        tickleMeElmophore.acquire(numTickleMeElmaphore);
        try {
            for (Map.Entry<RegionName, RegionDelta> e : regionDeltas.entrySet()) {
                if (e.getValue().compacting.get() != null) {
                    LOG.warn("Ingress is faster than we can compact!");
                    return;
                }
            }
            LOG.info("Compacting delta regions...");
            for (Map.Entry<RegionName, RegionDelta> e : regionDeltas.entrySet()) {
                final RegionDelta regionDelta = new RegionDelta(e.getKey(), deltaWAL, e.getValue());
                regionDeltas.put(e.getKey(), regionDelta);
                futures.add(compactionThreads.submit(new Callable<Long>() {

                    @Override
                    public Long call() throws Exception {
                        try {
                            return regionDelta.compact(regionProvider);
                        } catch (Exception x) {
                            LOG.error("Failed to compact:" + regionDelta, x);
                            // TODO what todo?
                            return null;
                        }
                    }
                }));
            }

        } finally {
            tickleMeElmophore.release(numTickleMeElmaphore);
        }

        long maxTxId = 0;
        boolean failed = false;
        for (Future<Long> f : futures) {
            Long txId = f.get();
            if (txId != null) {
                if (txId > maxTxId) {
                    maxTxId = txId;
                }
            } else {
                failed = true;
            }
        }
        if (!failed) {
            // TODO fix how do we do this an fix the fps in the RegionDeltas?
            //deltaWAL.compact(maxTxId);
            LOG.info("Compacted delta regions.");
        } else {
            LOG.warn("Compaction of delta regiond FAILED.");
        }

    }

    @Override
    public RowsChanged update(RegionName regionName, WALStorage storage, WALStorageUpdateMode updateMode, Scannable<WALValue> updates) throws Exception {

        final AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);
        final Map<WALKey, WALValue> apply = new ConcurrentHashMap<>();
        final Map<WALKey, WALPointer> removes = new HashMap<>();
        final Map<WALKey, WALPointer> clobbers = new HashMap<>();

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

        tickleMeElmophore.acquire();
        try {
            RowsChanged rowsChanged;

            // only grabbing pointers means our removes and clobbers don't include the old values, but for now this is more efficient.
            WALPointer[] currentPointers = getPointers(regionName, storage, keys, values);
            for (int i = 0; i < keys.size(); i++) {
                WALKey key = keys.get(i);
                WALPointer currentPointer = currentPointers[i];
                WALValue update = values.get(i);
                if (currentPointer == null) {
                    apply.put(key, update);
                    if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                        oldestAppliedTimestamp.set(update.getTimestampId());
                    }
                } else if (currentPointer.getTimestampId() < update.getTimestampId()) {
                    apply.put(key, update);
                    if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                        oldestAppliedTimestamp.set(update.getTimestampId());
                    }
                    clobbers.put(key, currentPointer);
                    if (update.getTombstoned() && !currentPointer.getTombstoned()) {
                        removes.put(key, currentPointer);
                    }
                }
            }

            List<Future<?>> futures = Lists.newArrayListWithCapacity(1);
            if (walReplicator != null && updateMode == WALStorageUpdateMode.replicateThenUpdate && !apply.isEmpty()) {
                futures.add(walReplicator.replicate(new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers)));
            }

            if (apply.isEmpty()) {
                rowsChanged = new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
            } else {

                RegionDelta delta = getRegionDeltas(regionName);
                synchronized (oneWriterAtATimeLock) {
                    DeltaWAL.DeltaWALApplied updateApplied = deltaWAL.update(regionName, apply);

                    for (Map.Entry<WALKey, WALValue> entry : apply.entrySet()) {
                        WALKey key = entry.getKey();
                        WALValue value = entry.getValue();
                        long pointer = UIO.bytesLong(updateApplied.keyToRowPointer.get(key));
                        WALPointer rowPointer = new WALPointer(pointer, value.getTimestampId(), value.getTombstoned());

                        WALPointer got = delta.getPointer(key);
                        if (got == null || got.getTimestampId() < value.getTimestampId()) {
                            delta.put(key, rowPointer);
                        } else {
                            apply.remove(key);
                        }
                    }
                    delta.appendTxFps(updateApplied.txId, updateApplied.keyToRowPointer.values());
                    rowsChanged = new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
                }

                if (walReplicator != null && updateMode == WALStorageUpdateMode.updateThenReplicate && !apply.isEmpty()) {
                    futures.add(walReplicator.replicate(new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers)));
                }
            }
            for (Future<?> future : futures) {
                future.get();
            }

            updateSinceLastCompaction.addAndGet(apply.size());
            return rowsChanged;
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public void takeRowUpdatesSince(RegionName regionName, WALStorage storage, long transactionId, final RowStream rowStream) throws Exception {
        boolean done;
        tickleMeElmophore.acquire();
        try {
            RegionDelta delta = getRegionDeltas(regionName);
            done = delta.takeRowUpdatesSince(transactionId, rowStream);
        } finally {
            tickleMeElmophore.release();
        }
        if (!done) {
            storage.takeRowUpdatesSince(transactionId, rowStream);
        }
    }

    @Override
    public WALValue get(RegionName regionName, WALStorage storage, WALKey key) throws Exception {
        WALValue got;
        tickleMeElmophore.acquire();
        try {
            got = getRegionDeltas(regionName).get(key);
        } finally {
            tickleMeElmophore.release();
        }
        if (got == null) {
            got = storage.get(key);
        }
        return got;

    }

    @Override
    public boolean containsKey(RegionName regionName, WALStorage storage, WALKey key) throws Exception {
        boolean contains;
        tickleMeElmophore.acquire();
        try {
            contains = getRegionDeltas(regionName).containsKey(key);
        } finally {
            tickleMeElmophore.release();
        }
        return contains || storage.containsKey(key);
    }

    private WALPointer[] getPointers(RegionName regionName, WALStorage storage, List<WALKey> keys, List<WALValue> values) throws Exception {
        WALKey[] consumableKeys = keys.toArray(new WALKey[keys.size()]);
        DeltaResult<WALPointer[]> deltas = getRegionDeltas(regionName).getPointers(consumableKeys);
        if (deltas.missed) {
            WALPointer[] pointers = deltas.result;
            WALPointer[] got = storage.getPointers(consumableKeys, values);
            for (int i = 0; i < pointers.length; i++) {
                if (pointers[i] == null) {
                    pointers[i] = got[i];
                }
            }
            return pointers;
        } else {
            return deltas.result;
        }
    }

    @Override
    public void rangeScan(final RegionName regionName, RangeScannable<WALValue> rangeScannable, WALKey from, WALKey to, final Scan<WALValue> scan)
        throws Exception {
        tickleMeElmophore.acquire();
        try {
            RegionDelta delta = getRegionDeltas(regionName);
            final DeltaPeekableElmoIterator iterator = delta.rangeScanIterator(from, to);
            rangeScannable.rangeScan(from, to, new Scan<WALValue>() {
                Map.Entry<WALKey, WALPointer> d;

                @Override
                public boolean row(long rowTxId, WALKey key, WALValue value) throws Exception {
                    if (d == null && iterator.hasNext()) {
                        d = iterator.next();
                    }
                    while (d != null && d.getKey().compareTo(key) <= 0) {
                        WALValue got = deltaWAL.hydrate(regionName, d.getValue());
                        if (!scan.row(-1, d.getKey(), got)) {
                            return false;
                        }
                        if (iterator.hasNext()) {
                            d = iterator.next();
                        } else {
                            iterator.eos();
                        }
                    }
                    return scan.row(-1, key, value);
                }
            });

            Map.Entry<WALKey, WALPointer> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    WALValue got = deltaWAL.hydrate(regionName, d.getValue());
                    scan.row(-1, d.getKey(), got);
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    WALValue got = deltaWAL.hydrate(regionName, d.getValue());
                    if (!scan.row(-1, d.getKey(), got)) {
                        return;
                    }
                }
            }
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public void rowScan(final RegionName regionName, Scannable<WALValue> scanable, final Scan<WALValue> scan) throws Exception {
        tickleMeElmophore.acquire();
        try {
            RegionDelta delta = getRegionDeltas(regionName);
            final DeltaPeekableElmoIterator iterator = delta.rowScanIterator();
            scanable.rowScan(new Scan<WALValue>() {
                Map.Entry<WALKey, WALPointer> d;

                @Override
                public boolean row(long rowTxId, WALKey key, WALValue value) throws Exception {
                    if (d == null && iterator.hasNext()) {
                        d = iterator.next();
                    }
                    while (d != null && d.getKey().compareTo(key) <= 0) {
                        WALValue got = deltaWAL.hydrate(regionName, d.getValue());
                        if (!scan.row(-1, d.getKey(), got)) {
                            return false;
                        }
                        if (iterator.hasNext()) {
                            d = iterator.next();
                        } else {
                            iterator.eos();
                        }
                    }
                    return scan.row(-1, key, value);
                }
            });

            Map.Entry<WALKey, WALPointer> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    WALValue got = deltaWAL.hydrate(regionName, d.getValue());
                    scan.row(-1, d.getKey(), got);
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    WALValue got = deltaWAL.hydrate(regionName, d.getValue());
                    if (!scan.row(-1, d.getKey(), got)) {
                        return;
                    }
                }
            }
        } finally {
            tickleMeElmophore.release();
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
        tickleMeElmophore.acquire();
        try {
            ArrayList<WALKey> keys = new ArrayList<>(getRegionDeltas(regionName).keySet());
            List<Boolean> containsKey = storage.containsKey(keys);
            count = Iterables.frequency(containsKey, Boolean.FALSE);
        } finally {
            tickleMeElmophore.release();
        }
        return count + storage.count();
    }

}
