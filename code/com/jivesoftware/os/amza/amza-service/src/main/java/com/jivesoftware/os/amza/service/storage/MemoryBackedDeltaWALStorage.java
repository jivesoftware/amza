package com.jivesoftware.os.amza.service.storage;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class MemoryBackedDeltaWALStorage implements DeltaWALStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config

    private final RowMarshaller<byte[]> rowMarshaller;
    private final DeltaWAL deltaWAL;
    private final WALReplicator walReplicator;
    private final ConcurrentHashMap<RegionName, RegionDelta> regionDeltas = new ConcurrentHashMap<>();
    private final Object oneWriterAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final ExecutorService compactionThreads;
    private final AtomicLong updateSinceLastCompaction;

    public MemoryBackedDeltaWALStorage(RowMarshaller<byte[]> rowMarshaller,
        DeltaWAL deltaWAL,
        WALReplicator walReplicator) {

        this.rowMarshaller = rowMarshaller;
        this.deltaWAL = deltaWAL;
        this.walReplicator = walReplicator;
        this.compactionThreads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setNameFormat("compact-deltas-%d").build()); // TODO pass in

        this.updateSinceLastCompaction = new AtomicLong();

    }

    @Override
    public void load(final RegionProvider regionProvider) throws Exception {
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
                            if (regionValue == null || regionValue.getTimestampId() <= value.getTimestampId()) {
                                WALValue got = delta.get(key);
                                if (got == null || got.getTimestampId() < value.getTimestampId()) {
                                    delta.put(key, new WALValue(UIO.longBytes(rowFP), value.getTimestampId(), value.getTombstoned()));
                                }
                            }
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

    public void compact(final RegionProvider regionProvider) throws Exception {
        if (true || updateSinceLastCompaction.longValue() < 100_000) { // TODO or some memory pressure BS!
            return;
        }

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
        boolean success = false;
        for (Future<Long> f : futures) {
            Long txId = f.get();
            success |= (txId == null);
            if (txId > maxTxId) {
                maxTxId = txId;
            }
        }
        if (success) {
            deltaWAL.compact(maxTxId);
        }
    }

    @Override
    public RowsChanged update(RegionName regionName, WALStorage storage, WALStorageUpdateMode updateMode, WALScanable updates) throws Exception {

        final AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);
        final Map<WALKey, WALValue> apply = new ConcurrentHashMap<>();
        final Map<WALKey, WALValue> removes = new HashMap<>();
        final Map<WALKey, WALValue> clobbers = new HashMap<>();

        final List<WALKey> keys = new ArrayList<>();
        final List<WALValue> values = new ArrayList<>();
        updates.rowScan(new WALScan() {
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

            List<WALValue> currentValues = getInternal(regionName, storage, keys);
            for (int i = 0; i < keys.size(); i++) {
                WALKey key = keys.get(i);
                WALValue current = currentValues.get(i);
                WALValue update = values.get(i);
                if (current == null) {
                    apply.put(key, update);
                    if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                        oldestAppliedTimestamp.set(update.getTimestampId());
                    }
                } else if (current.getTimestampId() < update.getTimestampId()) {
                    apply.put(key, update);
                    if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                        oldestAppliedTimestamp.set(update.getTimestampId());
                    }
                    clobbers.put(key, current);
                    if (update.getTombstoned() && !current.getTombstoned()) {
                        removes.put(key, current);
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

                DeltaWAL.DeltaWALApplied updateApplied = deltaWAL.update(regionName, oldestAppliedTimestamp, apply);

                RegionDelta delta = getRegionDeltas(regionName);
                synchronized (oneWriterAtATimeLock) {
                    for (Map.Entry<WALKey, WALValue> entry : apply.entrySet()) {
                        WALKey key = entry.getKey();
                        WALValue value = entry.getValue();
                        byte[] rowPointer = updateApplied.keyToRowPointer.get(key);
                        WALValue rowValue = new WALValue(rowPointer, value.getTimestampId(), value.getTombstoned());

                        WALValue got = delta.get(key);
                        if (got == null) {
                            delta.put(key, rowValue);
                        } else if (got.getTimestampId() < value.getTimestampId()) {
                            delta.put(key, rowValue);
                        } else {
                            apply.remove(key);
                        }
                    }
                    delta.appendTxFps(updateApplied.txId, updateApplied.keyToRowPointer.values());
                    rowsChanged = new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
                }

                if (walReplicator != null && updateMode == WALStorageUpdateMode.updateThenReplicate && !apply.isEmpty()) {
                    walReplicator.replicate(new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers));
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
        if (got != null) {
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
        return contains ? true : storage.containsKey(key);

    }

    private List<WALValue> getInternal(RegionName regionName, WALStorage storage, List<WALKey> keys) throws Exception {
        DeltaResult<List<WALValue>> deltas = getRegionDeltas(regionName).get(keys);
        if (deltas.missed) {
            List<WALValue> got = storage.get(keys);
            List<WALValue> values = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                WALValue delta = deltas.result.get(i);
                if (delta == null) {
                    values.add(got.get(i));
                } else {
                    values.add(delta);
                }
            }
            return values;
        } else {
            return deltas.result;
        }
    }

    @Override
    public void rangeScan(RegionName regionName, RangeScannable rangeScannable, WALKey from, WALKey to, final WALScan walScan) throws Exception {
        tickleMeElmophore.acquire();
        try {
            RegionDelta delta = getRegionDeltas(regionName);
            final PeekableElmoIterator iterator = delta.rangeScanIterator(from, to);
            rangeScannable.rangeScan(from, to, new WALScan() {
                Map.Entry<WALKey, WALValue> d;

                @Override
                public boolean row(long rowTxId, WALKey key, WALValue value) throws Exception {
                    if (d == null && iterator.hasNext()) {
                        d = iterator.next();
                    }
                    while (d != null && d.getKey().compareTo(key) <= 0) {
                        if (!walScan.row(-1, d.getKey(), d.getValue())) {
                            return false;
                        }
                        if (iterator.hasNext()) {
                            d = iterator.next();
                        } else {
                            iterator.eos();
                        }
                    }
                    return walScan.row(-1, key, value);
                }
            });

            Map.Entry<WALKey, WALValue> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    walScan.row(-1, d.getKey(), d.getValue());
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    if (!walScan.row(-1, d.getKey(), d.getValue())) {
                        return;
                    }
                }
            }
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public void rowScan(RegionName regionName, WALScanable scanable, final WALScan walScan) throws Exception {
        tickleMeElmophore.acquire();
        try {
            RegionDelta delta = getRegionDeltas(regionName);
            final PeekableElmoIterator iterator = delta.rowScanIterator();
            scanable.rowScan(new WALScan() {
                Map.Entry<WALKey, WALValue> d;

                @Override
                public boolean row(long rowTxId, WALKey key, WALValue value) throws Exception {
                    if (d == null && iterator.hasNext()) {
                        d = iterator.next();
                    }
                    while (d != null && d.getKey().compareTo(key) <= 0) {
                        if (!walScan.row(-1, d.getKey(), d.getValue())) {
                            return false;
                        }
                        if (iterator.hasNext()) {
                            d = iterator.next();
                        } else {
                            iterator.eos();
                        }
                    }
                    return walScan.row(-1, key, value);
                }
            });

            Map.Entry<WALKey, WALValue> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    walScan.row(-1, d.getKey(), d.getValue());
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    if (!walScan.row(-1, d.getKey(), d.getValue())) {
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
    public long size(RegionName regionName, WALStorage storage) throws Exception {
        int count = 0;
        tickleMeElmophore.acquire();
        try {
            ArrayList<WALKey> keys = new ArrayList<>(getRegionDeltas(regionName).keySet());
            List<Boolean> containsKey = storage.containsKey(keys);
            count = Iterables.frequency(containsKey, Boolean.FALSE);
        } finally {
            tickleMeElmophore.release();
        }
        return count + storage.size();
    }

    static final class PeekableElmoIterator implements Iterator<Map.Entry<WALKey, WALValue>> {

        private final Iterator<Map.Entry<WALKey, WALValue>> iterator;
        private final Iterator<Map.Entry<WALKey, WALValue>> compactingIterator;
        private Map.Entry<WALKey, WALValue> last;
        private Map.Entry<WALKey, WALValue> iNext;
        private Map.Entry<WALKey, WALValue> cNext;

        public PeekableElmoIterator(Iterator<Map.Entry<WALKey, WALValue>> iterator,
            Iterator<Map.Entry<WALKey, WALValue>> compactingIterator) {
            this.iterator = iterator;
            this.compactingIterator = compactingIterator;
        }

        public void eos() {
            last = null;
        }

        public Map.Entry<WALKey, WALValue> last() {
            return last;
        }

        @Override
        public boolean hasNext() {
            return (iNext != null || cNext != null) || iterator.hasNext() || compactingIterator.hasNext();
        }

        @Override
        public Map.Entry<WALKey, WALValue> next() {
            if (iNext == null && iterator.hasNext()) {
                iNext = iterator.next();
            }
            if (cNext == null && compactingIterator.hasNext()) {
                cNext = compactingIterator.next();
            }
            if (iNext != null && cNext != null) {
                int compare = iNext.getKey().compareTo(cNext.getKey());
                if (compare == 0) {
                    if (iNext.getValue().getTimestampId() > cNext.getValue().getTimestampId()) {
                        last = iNext;
                    } else {
                        last = cNext;
                    }
                    iNext = null;
                    cNext = null;
                } else if (compare < 0) {
                    last = iNext;
                    iNext = null;
                } else {
                    last = cNext;
                    cNext = null;
                }
            } else if (iNext != null) {
                last = iNext;
                iNext = null;
            } else if (cNext != null) {
                last = cNext;
                cNext = null;
            } else {
                throw new NoSuchElementException();
            }
            return last;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported ever!");
        }

    }

    static class RegionDelta {

        private final RegionName regionName;
        private final DeltaWAL deltaWAL;
        private final ConcurrentNavigableMap<WALKey, WALValue> index = new ConcurrentSkipListMap<>();
        private final ConcurrentSkipListMap<Long, Collection<byte[]>> txIdWAL = new ConcurrentSkipListMap<>();
        private final AtomicReference<RegionDelta> compacting;

        RegionDelta(RegionName regionName, DeltaWAL deltaWAL, RegionDelta compacting) {
            this.regionName = regionName;
            this.deltaWAL = deltaWAL;
            this.compacting = new AtomicReference<>(compacting);
        }

        WALValue get(WALKey key) throws Exception {
            WALValue got = index.get(key);
            if (got == null) {
                RegionDelta regionDelta = compacting.get();
                if (regionDelta != null) {
                    return regionDelta.get(key);
                }
                return null;
            }
            return deltaWAL.hydrate(regionName, got.getValue());
        }

        DeltaResult<List<WALValue>> get(List<WALKey> keys) throws Exception {
            boolean missed = false;
            List<WALValue> result = new ArrayList<>(keys.size());
            for (WALKey key : keys) {
                WALValue got = get(key);
                missed |= (got == null);
                result.add(got);
            }
            return new DeltaResult<>(missed, result);
        }

        boolean containsKey(WALKey key) {
            if (index.get(key) != null) {
                return true;
            }
            RegionDelta regionDelta = compacting.get();
            if (regionDelta != null) {
                return regionDelta.containsKey(key);
            }
            return false;
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

        void put(WALKey key, WALValue rowValue) {
            index.put(key, rowValue);
        }

        Set<WALKey> keySet() {
            Set<WALKey> keySet = index.keySet();
            RegionDelta regionDelta = compacting.get();
            if (regionDelta != null) {
                HashSet<WALKey> all = new HashSet<>(keySet);
                all.addAll(regionDelta.keySet());
                return all;
            }
            return keySet;
        }

        PeekableElmoIterator rangeScanIterator(WALKey from, WALKey to) {
            Iterator<Map.Entry<WALKey, WALValue>> iterator = index.subMap(from, to).entrySet().iterator();
            Iterator<Map.Entry<WALKey, WALValue>> compactingIterator = Iterators.emptyIterator();
            RegionDelta regionDelta = compacting.get();
            if (regionDelta != null) {
                compactingIterator = regionDelta.index.subMap(from, to).entrySet().iterator();
            }
            return new PeekableElmoIterator(iterator, compactingIterator);
        }

        PeekableElmoIterator rowScanIterator() {
            Iterator<Map.Entry<WALKey, WALValue>> iterator = index.entrySet().iterator();
            Iterator<Map.Entry<WALKey, WALValue>> compactingIterator = Iterators.emptyIterator();
            RegionDelta regionDelta = compacting.get();
            if (regionDelta != null) {
                compactingIterator = regionDelta.index.entrySet().iterator();
            }
            return new PeekableElmoIterator(iterator, compactingIterator);
        }

        void appendTxFps(long rowTxId, long rowFP) {
            Collection<byte[]> fps = txIdWAL.get(rowTxId);
            if (fps == null) {
                fps = new ArrayList<>();
                txIdWAL.put(rowTxId, fps);
            }
            fps.add(UIO.longBytes(rowFP));
        }

        void appendTxFps(long rowTxId, Collection<byte[]> rowFPs) {
            Collection<byte[]> fps = txIdWAL.get(rowTxId);
            if (fps == null) {
                fps = new ArrayList<>();
                txIdWAL.put(rowTxId, fps);
            }
            fps.addAll(rowFPs);
        }

        boolean takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
            ConcurrentNavigableMap<Long, Collection<byte[]>> tailMap = txIdWAL.tailMap(transactionId);
            deltaWAL.takeRows(tailMap, rowStream);

            if (!txIdWAL.isEmpty() && txIdWAL.firstEntry().getKey() <= transactionId) {
                return true;
            }

            RegionDelta regionDelta = compacting.get();
            if (regionDelta != null) {
                if (regionDelta.takeRowUpdatesSince(transactionId, rowStream)) {
                    return true;
                }
            }
            return false;
        }

        long compact(RegionProvider regionProvider) throws Exception {
            final RegionDelta compact = compacting.get();
            long largestTxId = 0;
            if (compact != null) {
                largestTxId = compact.txIdWAL.lastKey();
                RegionStore regionStore = regionProvider.getRegionStore(compact.regionName);
                regionStore.directCommit(largestTxId, new WALScanable() {

                    @Override
                    public void rowScan(WALScan walScan) {
                        for (Map.Entry<WALKey, WALValue> e : compact.index.entrySet()) {
                            try {
                                if (!walScan.row(-1, e.getKey(), compact.deltaWAL.hydrate(compact.regionName, e.getValue().getValue()))) {
                                    break;
                                }
                            } catch (Throwable ex) {
                                throw new RuntimeException("Error while streaming entry set.", ex);
                            }
                        }
                    }
                });

            }
            compacting.set(null);
            return largestTxId;
        }
    }

    public static class DeltaResult<R> {

        public final boolean missed;
        public final R result;

        public DeltaResult(boolean missed, R result) {
            this.missed = missed;
            this.result = result;
        }

    }
}
