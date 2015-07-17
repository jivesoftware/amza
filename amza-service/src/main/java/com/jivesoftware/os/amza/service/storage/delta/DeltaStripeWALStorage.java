package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.delta.DeltaWAL.KeyValueHighwater;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedStatus;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RangeScannable;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scannable;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.KeyedTimestampId;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.TimestampKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
import com.jivesoftware.os.amza.shared.wal.WALTimestampId;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.commons.lang.mutable.MutableInt;

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
    private final Object awakeCompactionsLock = new Object();
    private final long mergeAfterNUpdates;
    private final ConcurrentHashMap<VersionedPartitionName, PartitionDelta> partitionDeltas = new ConcurrentHashMap<>();
    private final Object oneWriterAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final ExecutorService mergeDeltaThreads;
    private final AtomicLong updateSinceLastMerge = new AtomicLong();
    private final AtomicBoolean merging = new AtomicBoolean(false);

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
        long mergeAfterNUpdates) {

        this.index = index;
        this.amzaStats = amzaStats;
        this.deltaWALFactory = deltaWALFactory;
        this.mergeAfterNUpdates = mergeAfterNUpdates;
        this.mergeDeltaThreads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), // TODO expose to config;
            new ThreadFactoryBuilder().setNameFormat("merge-deltas-" + index + "-%d").build());
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

    public void load(TxPartitionStatus txPartitionStatus,
        PartitionIndex partitionIndex,
        PrimaryRowMarshaller<byte[]> primaryRowMarshaller) throws Exception {

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
                        mergeDelta(partitionIndex, deltaWAL.get(), () -> wal);
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
                                VersionedStatus localStatus = txPartitionStatus.getLocalStatus(versionedPartitionName.getPartitionName());
                                if (localStatus != null && localStatus.version == versionedPartitionName.getPartitionVersion()) {
                                    PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                                    if (partitionStore == null) {
                                        LOG.warn("Dropping values on the floor for versionedPartitionName:{} "
                                            + " this is typical when loading an expunged partition", versionedPartitionName);
                                        // TODO ensure partitionIsExpunged?
                                    } else {
                                        acquireOne();
                                        try {
                                            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
                                            TimestampedValue partitionValue = partitionStore.get(keyBytes);
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
                                    updateSinceLastMerge.incrementAndGet();
                                }
                                return true;
                            });

                        }
                        return true;
                    });

                }
            }
        }
        amzaStats.deltaStripeLoad(index, updateSinceLastMerge.get(), (double) updateSinceLastMerge.get() / (double) mergeAfterNUpdates);

        if (updateSinceLastMerge.get() > mergeAfterNUpdates) {
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
            long highestTxId = partitionDelta.highestTxId();
            if (highestTxId > -1) {
                return highestTxId;
            }
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
            partitionDelta = new PartitionDelta(versionedPartitionName, wal, null);
            PartitionDelta had = partitionDeltas.putIfAbsent(versionedPartitionName, partitionDelta);
            if (had != null) {
                partitionDelta = had;
            }
        }
        return partitionDelta;
    }

    public boolean mergeable() {
        return updateSinceLastMerge.get() > mergeAfterNUpdates;
    }

    public void merge(PartitionIndex partitionIndex, boolean force) throws Exception {
        if (!force && !mergeable()) {
            return;
        }

        if (!merging.compareAndSet(false, true)) {
            LOG.warn("Trying to merge DeltaStripe:" + partitionIndex + " while another merge is already in progress.");
            return;
        }
        amzaStats.beginCompaction("Merging Delta Stripe:" + index);
        try {
            mergeDelta(partitionIndex, deltaWAL.get(), deltaWALFactory::create);
        } finally {
            merging.set(false);
            amzaStats.endCompaction("Merging Delta Stripe:" + index);
        }
    }

    private void mergeDelta(final PartitionIndex partitionIndex, DeltaWAL wal, Callable<DeltaWAL> newWAL) throws Exception {
        final List<Future<Boolean>> futures = new ArrayList<>();
        acquireAll();
        try {
            for (Map.Entry<VersionedPartitionName, PartitionDelta> e : partitionDeltas.entrySet()) {
                if (e.getValue().merging.get() != null) {
                    LOG.warn("Ingress is faster than we can merge!");
                    return;
                }
            }
            LOG.info("Merging delta partitions...");
            DeltaWAL newDeltaWAL = newWAL.call();
            deltaWAL.set(newDeltaWAL);
            updateSinceLastMerge.set(0);

            AtomicLong mergeable = new AtomicLong();
            AtomicLong merged = new AtomicLong();
            AtomicLong unmerged = new AtomicLong();

            for (Map.Entry<VersionedPartitionName, PartitionDelta> e : partitionDeltas.entrySet()) {
                PartitionDelta mergeableDelta = e.getValue();
                unmerged.addAndGet(mergeableDelta.size());
                mergeable.incrementAndGet();
                PartitionDelta currentDelta = new PartitionDelta(e.getKey(),
                    newDeltaWAL, mergeableDelta);
                partitionDeltas.put(e.getKey(), currentDelta);

                futures.add(mergeDeltaThreads.submit(() -> {
                    try {
                        long count = currentDelta.merge(partitionIndex);
                        amzaStats.deltaStripeMerge(index,
                            mergeable.decrementAndGet(),
                            (double) (unmerged.get() - merged.addAndGet(count)) / (double) unmerged.get());
                        return true;
                    } catch (Exception x) {
                        LOG.error("Failed to merge:" + currentDelta, x);
                        return false;
                    }
                }));
            }
            amzaStats.deltaStripeMerge(index, 0, 0);
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

        if (merging.get() && updateSinceLastMerge.get() > mergeAfterNUpdates) {
            throw new DeltaOverCapacityException();
        }

        final AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);

        final Map<WALKey, WALValue> apply = new LinkedHashMap<>();

        final List<KeyedTimestampId> removes = new ArrayList<>();
        final List<KeyedTimestampId> clobbers = new ArrayList<>();

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

            getPointers(versionedPartitionName, storage, (stream) -> {
                for (int i = 0; i < keys.size(); i++) {
                    byte[] key = keys.get(i);
                    WALValue update = values.get(i);
                    if (!stream.stream(key, update.getValue(), update.getTimestampId(), update.getTombstoned())) {
                        return false;
                    }
                }
                return true;
            }, (key, value, valueTimestamp, valueTombstone, pointerTimestamp, pointerTombstoned, pointerFp) -> {
                WALKey walKey = new WALKey(key);
                WALValue walValue = new WALValue(value, valueTimestamp, valueTombstone);
                if (pointerFp == -1) {
                    apply.put(walKey, walValue);
                    if (oldestAppliedTimestamp.get() > valueTimestamp) {
                        oldestAppliedTimestamp.set(valueTimestamp);
                    }
                } else if (pointerTimestamp < valueTimestamp) {
                    apply.put(walKey, walValue);
                    if (oldestAppliedTimestamp.get() > valueTimestamp) {
                        oldestAppliedTimestamp.set(valueTimestamp);
                    }
                    WALTimestampId walTimestampId = new WALTimestampId(pointerTimestamp, pointerTombstoned);
                    KeyedTimestampId keyedTimestampId = new KeyedTimestampId(walKey.getKey(), walTimestampId.getTimestampId(), walTimestampId.getTombstoned());
                    clobbers.add(keyedTimestampId);
                    if (valueTombstone && !pointerTombstoned) {
                        removes.add(keyedTimestampId);
                    }
                }
                return true;
            });

            if (apply.isEmpty()) {
                rowsChanged = new RowsChanged(versionedPartitionName, oldestAppliedTimestamp.get(), Collections.emptyMap(), removes, clobbers, -1);
            } else {
                PartitionDelta delta = getPartitionDelta(versionedPartitionName);
                WALHighwater partitionHighwater = null;
                if (delta.shouldWriteHighwater()) {
                    partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
                }
                DeltaWAL.DeltaWALApplied updateApplied;
                synchronized (oneWriterAtATimeLock) {
                    updateApplied = wal.update(versionedPartitionName, apply, partitionHighwater);

                    for (int i = 0; i < updateApplied.fps.length; i++) {
                        KeyValueHighwater keyValueHighwater = updateApplied.keyValueHighwaters[i];
                        long fp = updateApplied.fps[i];
                        WALPointer got = delta.getPointer(keyValueHighwater.key);
                        if (got == null || got.getTimestampId() < keyValueHighwater.valueTimestamp) {
                            delta.put(fp, keyValueHighwater.key, keyValueHighwater.value, keyValueHighwater.valueTimestamp, keyValueHighwater.valueTombstone,
                                keyValueHighwater.highwater);
                        } else {
                            apply.remove(new WALKey(keyValueHighwater.key));
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

            long unmergedUpdates = updateSinceLastMerge.addAndGet(apply.size());
            amzaStats.deltaStripeLoad(index, unmergedUpdates, (double) unmergedUpdates / (double) mergeAfterNUpdates);
            if (unmergedUpdates > mergeAfterNUpdates) {
                synchronized (awakeCompactionsLock) {
                    awakeCompactionsLock.notifyAll();
                }
            }
            return rowsChanged;
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

        if ((lowestTxId == -1 || lowestTxId > transactionId) && !storage.takeRowUpdatesSince(transactionId, rowStream)) {
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

    public boolean takeAllRows(VersionedPartitionName versionedPartitionName, WALStorage storage, RowStream rowStream)
        throws Exception {

        if (!storage.takeAllRows(rowStream)) {
            return false;
        }

        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            return delta.takeRowsFromTransactionId(0, rowStream);
        } finally {
            releaseOne();
        }
    }

    public WALValue get(VersionedPartitionName versionedPartitionName, WALStorage storage, byte[] key) throws Exception {
        WALValue[] walValue = new WALValue[1];
        get(versionedPartitionName, storage, (stream) -> {
            return stream.stream(key);
        }, (byte[] key1, byte[] value, long timestamp) -> {
            if (value != null) {
                walValue[0] = new WALValue(value, timestamp, false);
            }
            return true;
        });
        return walValue[0];
    }

    public boolean get(VersionedPartitionName versionedPartitionName, WALStorage storage, WALKeys keys, TimestampKeyValueStream stream) throws Exception {
        acquireOne();
        try {
            PartitionDelta partitionDelta = getPartitionDelta(versionedPartitionName);
            return storage.get(
                (storageStream)
                    -> partitionDelta.get(keys, (key, value, valueTimestamp, valueTombstoned) -> {
                    if (value == null) {
                        return storageStream.stream(key);
                    } else {
                        if (valueTombstoned) {
                            return stream.stream(key, null, -1);
                        } else {
                            return stream.stream(key, value, valueTimestamp);
                        }
                    }
                }),
                (key, value, valueTimestamp, valueTombstoned) -> {
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

    public boolean containsKeys(VersionedPartitionName versionedPartitionName, WALStorage storage, WALKeys keys, KeyContainedStream stream) throws Exception {
        acquireOne();
        try {
            return storage.containsKeys(
                storageKeyStream -> getPartitionDelta(versionedPartitionName).containsKeys(keys,
                    (key, tombstoned, exists) -> {
                        if (exists) {
                            return stream.stream(key, !tombstoned);
                        } else {
                            return storageKeyStream.stream(key);
                        }
                    }),
                stream);
        } finally {
            releaseOne();
        }
    }

    private boolean getPointers(VersionedPartitionName versionedPartitionName,
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
        byte[] from,
        byte[] to,
        KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            final DeltaPeekableElmoIterator iterator = delta.rangeScanIterator(from, to);
            rangeScannable.rangeScan(from, to, new LatestWinnernator(iterator, keyValueStream));

            Map.Entry<byte[], WALValue> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    WALValue got = d.getValue();
                    keyValueStream.stream(d.getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned());
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    WALValue got = d.getValue();
                    if (!keyValueStream.stream(d.getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned())) {
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

            Map.Entry<byte[], WALValue> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                if (d != null) {
                    WALValue got = d.getValue();
                    if (!keyValueStream.stream(d.getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned())) {
                        return false;
                    }
                }
                while (iterator.hasNext()) {
                    d = iterator.next();
                    WALValue got = d.getValue();
                    if (!keyValueStream.stream(d.getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned())) {
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
        MutableInt count = new MutableInt(0);
        acquireOne();
        try {
            storage.containsKeys(stream -> getPartitionDelta(versionedPartitionName).keys(stream::stream),
                (key, contained) -> {
                    if (!contained) {
                        count.increment();
                    }
                    return true;
                });
        } finally {
            releaseOne();
        }
        return count.intValue() + storage.count();
    }

    static class LatestWinnernator implements KeyValueStream {

        private final DeltaPeekableElmoIterator iterator;
        private final KeyValueStream keyValueStream;
        private Map.Entry<byte[], WALValue> d;

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
            while (d != null && WALKey.compare(d.getKey(), key) <= 0) {
                WALValue got = d.getValue();
                if (Arrays.equals(d.getKey(), key)) {
                    needsKey = false;
                }
                if (!keyValueStream.stream(d.getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned())) {
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
