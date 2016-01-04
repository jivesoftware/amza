package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedState;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.service.SickThreads;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.delta.DeltaWAL.KeyValueHighwater;
import com.jivesoftware.os.amza.api.scan.RangeScannable;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.scan.Scannable;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionFamily;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyValues;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.api.wal.KeyedTimestampId;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALPointer;
import com.jivesoftware.os.amza.api.wal.WALTimestampId;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * @author jonathan.colt
 */
public class DeltaStripeWALStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config

    private final int index;
    private final AmzaStats amzaStats;
    private final SickThreads sickThreads;
    private final DeltaWALFactory deltaWALFactory;
    private final AtomicReference<DeltaWAL> deltaWAL = new AtomicReference<>();
    private final Object awakeCompactionsLock = new Object();
    private final long mergeAfterNUpdates;
    private final ConcurrentHashMap<VersionedPartitionName, PartitionDelta> partitionDeltas = new ConcurrentHashMap<>();
    private final Object oneWriterAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final ExecutorService mergeDeltaThreads;
    private final AtomicLong updateSinceLastMerge = new AtomicLong();
    private final AtomicLong merging = new AtomicLong(0);

    private final Reentrant reentrant = new Reentrant();

    public void hackTruncation(int numBytes) {
        deltaWAL.get().hackTruncation(numBytes);
    }

    static class Reentrant extends ThreadLocal<Integer> {

        @Override
        protected Integer initialValue() {
            return 0;
        }
    }

    public DeltaStripeWALStorage(int index, AmzaStats amzaStats, SickThreads sickThreads, DeltaWALFactory deltaWALFactory, long mergeAfterNUpdates) {
        this.sickThreads = sickThreads;
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

    public void load(TxPartitionState txPartitionState,
        PartitionIndex partitionIndex,
        PrimaryRowMarshaller primaryRowMarshaller) throws Exception {

        LOG.info("Reloading deltas...");
        long start = System.currentTimeMillis();
        synchronized (oneWriterAtATimeLock) {
            List<DeltaWAL> deltaWALs = deltaWALFactory.list();
            if (deltaWALs.isEmpty()) {
                deltaWAL.set(deltaWALFactory.create());
            } else {
                for (int i = 0; i < deltaWALs.size(); i++) {
                    DeltaWAL wal = deltaWALs.get(i);
                    if (i > 0) {
                        mergeDelta(partitionIndex, deltaWAL.get(), () -> wal);
                    }
                    deltaWAL.set(wal);
                    WALKey.decompose(
                        (WALKey.TxFpRawKeyValueEntries<VersionedPartitionName>) txRawKeyEntryStream -> primaryRowMarshaller.fromRows(
                            txFpRowStream -> {
                                wal.load((rowFP, rowTxId, rowType, rawRow) -> {
                                    if (rowType.isPrimary()) {
                                        if (!txFpRowStream.stream(rowTxId, rowFP, rowType, rawRow)) {
                                            return false;
                                        }
                                    }
                                    return true;
                                });
                                return true;
                            },
                            (rowTxId, rowFP, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row) -> {
                                VersionedPartitionName versionedPartitionName = VersionedPartitionName.fromBytes(prefix);
                                VersionedState localState = txPartitionState.getLocalVersionedState(versionedPartitionName.getPartitionName());
                                if (localState != null && localState.getPartitionVersion() == versionedPartitionName.getPartitionVersion()) {
                                    return txRawKeyEntryStream.stream(rowTxId, rowFP, rowType, key,
                                        value, valueTimestamp, valueTombstoned, valueVersion, versionedPartitionName);
                                }
                                return true;
                            }),
                        (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, versionedPartitionName) -> {
                            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                            if (partitionStore == null) {
                                LOG.warn("Dropping values on the floor for versionedPartitionName:{} "
                                    + " this is typical when loading an expunged partition", versionedPartitionName);
                                // TODO ensure partitionIsExpunged?
                            } else {
                                acquireOne();
                                try {
                                    PartitionDelta delta = getPartitionDelta(versionedPartitionName);
                                    TimestampedValue partitionValue = partitionStore.getTimestampedValue(prefix, key);
                                    if (partitionValue == null
                                    || CompareTimestampVersions.compare(partitionValue.getTimestampId(), partitionValue.getVersion(),
                                        valueTimestamp, valueVersion) < 0) {
                                        WALPointer got = delta.getPointer(prefix, key);
                                        if (got == null
                                        || CompareTimestampVersions.compare(got.getTimestampId(), got.getVersion(),
                                            valueTimestamp, valueVersion) < 0) {
                                            delta.put(fp, prefix, key, valueTimestamp, valueTombstoned, valueVersion);
                                            delta.onLoadAppendTxFp(prefix, txId, fp);
                                            updateSinceLastMerge.incrementAndGet();
                                            return true;
                                        }
                                    }
                                } finally {
                                    releaseOne();
                                }
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

        long had = updateSinceLastMerge.get();
        if (!merging.compareAndSet(0, had)) {
            LOG.warn("Trying to merge DeltaStripe:" + partitionIndex + " while another merge is already in progress.");
            return;
        }
        amzaStats.beginCompaction(CompactionFamily.merge, "Delta Stripe:" + index);
        try {
            if (mergeDelta(partitionIndex, deltaWAL.get(), deltaWALFactory::create)) {
                merging.set(0);
            }
        } finally {
            amzaStats.endCompaction(CompactionFamily.merge, "Delta Stripe:" + index);
        }
    }

    private boolean mergeDelta(final PartitionIndex partitionIndex, DeltaWAL wal, Callable<DeltaWAL> newWAL) throws Exception {
        final List<Future<Boolean>> futures = new ArrayList<>();
        acquireAll();
        try {
            for (Map.Entry<VersionedPartitionName, PartitionDelta> e : partitionDeltas.entrySet()) {
                if (e.getValue().merging.get() != null) {
                    LOG.warn("Ingress is faster than we can merge!");
                    return false;
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
                PartitionDelta currentDelta = new PartitionDelta(e.getKey(), newDeltaWAL, mergeableDelta);
                partitionDeltas.put(e.getKey(), currentDelta);

                futures.add(mergeDeltaThreads.submit(() -> {
                    boolean sick = false;
                    while (true) {
                        try {
                            long count = currentDelta.merge(partitionIndex);
                            amzaStats.deltaStripeMerge(index,
                                mergeable.decrementAndGet(),
                                (unmerged.get() - merged.addAndGet(count)) / (double) unmerged.get());
                            if (sick) {
                                sickThreads.recovered();
                            }
                            return true;
                        } catch (Throwable x) {
                            sick = true;
                            sickThreads.sick(x);
                            LOG.error("Failed to merge:{} Things are going south! We will retry in the off chance the issue can be resovled in 30sec.",
                                currentDelta, x);
                            Thread.sleep(30_000);
                        }
                    }
                }));
            }
            amzaStats.deltaStripeMerge(index, 0, 0);
        } catch (Exception x) {
            sickThreads.sick(x);
            LOG.error(
                "This is catastrophic."
                + " We have permanently park this thread."
                + " This delta {} can no longer accept writes."
                + " You likely need to restart this instance",
                new Object[]{index}, x);
            LockSupport.park();
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
        return !failed;
    }

    public RowsChanged update(RowType rowType,
        HighwaterStorage highwaterStorage,
        VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        byte[] prefix,
        Commitable updates,
        WALUpdated updated) throws Exception {

        if ((merging.get() > 0 && merging.get() + updateSinceLastMerge.get() > (2 * mergeAfterNUpdates))
            || updateSinceLastMerge.get() > (2 * mergeAfterNUpdates)) {
            throw new DeltaOverCapacityException();
        }

        final Map<WALKey, WALValue> apply = new LinkedHashMap<>();

        final List<KeyedTimestampId> removes = new ArrayList<>();
        final List<KeyedTimestampId> clobbers = new ArrayList<>();

        final List<byte[]> keys = new ArrayList<>();
        final List<WALValue> values = new ArrayList<>();
        updates.commitable(null, (transactionId, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
            keys.add(key);
            values.add(new WALValue(rowType, value, valueTimestamp, valueTombstone, valueVersion));
            return true;
        });

        acquireOne();
        try {
            DeltaWAL<?> wal = deltaWAL.get();
            RowsChanged rowsChanged;

            getPointers(versionedPartitionName,
                storage,
                (stream) -> {
                    for (int i = 0; i < keys.size(); i++) {
                        byte[] key = keys.get(i);
                        WALValue update = values.get(i);
                        if (!stream.stream(rowType, prefix, key, update.getValue(), update.getTimestampId(), update.getTombstoned(), update.getVersion())) {
                            return false;
                        }
                    }
                    return true;
                },
                (_rowType, _prefix, key, value, valueTimestamp, valueTombstone, valueVersion, pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp)
                -> {
                WALKey walKey = new WALKey(prefix, key);
                WALValue walValue = new WALValue(rowType, value, valueTimestamp, valueTombstone, valueVersion);
                if (pointerFp == -1) {
                    apply.put(walKey, walValue);
                } else if (CompareTimestampVersions.compare(pointerTimestamp, pointerVersion, valueTimestamp, valueVersion) < 0) {
                    apply.put(walKey, walValue);
                    WALTimestampId walTimestampId = new WALTimestampId(pointerTimestamp, pointerTombstoned);
                    KeyedTimestampId keyedTimestampId = new KeyedTimestampId(prefix, key, walTimestampId.getTimestampId(), walTimestampId.getTombstoned());
                    clobbers.add(keyedTimestampId);
                    if (valueTombstone && !pointerTombstoned) {
                        removes.add(keyedTimestampId);
                    }
                } else {
                    amzaStats.deltaFirstCheckRemoves.incrementAndGet();
                }
                return true;
            });

            long appliedCount = 0;
            if (apply.isEmpty()) {
                rowsChanged = new RowsChanged(versionedPartitionName, Collections.emptyMap(), removes, clobbers, -1, -1);
            } else {
                PartitionDelta delta = getPartitionDelta(versionedPartitionName);
                WALHighwater partitionHighwater = null;
                if (delta.shouldWriteHighwater()) {
                    partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
                }
                DeltaWAL.DeltaWALApplied updateApplied;
                synchronized (oneWriterAtATimeLock) {
                    updateApplied = wal.update(rowType, versionedPartitionName, apply, partitionHighwater);
                    appliedCount = apply.size();

                    for (int i = 0; i < updateApplied.fps.length; i++) {
                        KeyValueHighwater keyValueHighwater = updateApplied.keyValueHighwaters[i];
                        long fp = updateApplied.fps[i];
                        WALPointer got = delta.getPointer(keyValueHighwater.prefix, keyValueHighwater.key);
                        if (got == null || CompareTimestampVersions.compare(got.getTimestampId(), got.getVersion(),
                            keyValueHighwater.valueTimestamp, keyValueHighwater.valueVersion) < 0) {
                            delta.put(fp,
                                keyValueHighwater.prefix,
                                keyValueHighwater.key,
                                keyValueHighwater.valueTimestamp,
                                keyValueHighwater.valueTombstone,
                                keyValueHighwater.valueVersion);
                        } else {
                            apply.remove(new WALKey(keyValueHighwater.prefix, keyValueHighwater.key));
                            amzaStats.deltaSecondCheckRemoves.incrementAndGet();
                        }
                    }
                    delta.appendTxFps(prefix, updateApplied.txId, updateApplied.fps);
                    rowsChanged = new RowsChanged(versionedPartitionName,
                        apply,
                        removes,
                        clobbers,
                        updateApplied.txId,
                        updateApplied.txId);
                }
                updated.updated(versionedPartitionName, updateApplied.txId);
            }

            long unmergedUpdates = updateSinceLastMerge.addAndGet(appliedCount);
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

    public boolean takeRowsFromTransactionId(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        byte[] prefix,
        long transactionId,
        RowStream rowStream) throws Exception {

        long lowestTxId;
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            lowestTxId = delta.lowestTxId(prefix);
        } finally {
            releaseOne();
        }

        if ((lowestTxId == -1 || lowestTxId > transactionId) && !storage.takeRowUpdatesSince(prefix, transactionId, rowStream)) {
            return false;
        }

        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            return delta.takeRowsFromTransactionId(prefix, transactionId, rowStream);
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

    public WALValue get(VersionedPartitionName versionedPartitionName, WALStorage storage, byte[] prefix, byte[] key) throws Exception {
        WALValue[] walValue = new WALValue[1];
        get(versionedPartitionName,
            storage,
            prefix,
            stream -> stream.stream(key),
            (rowType, _prefix, _key, value, timestamp, tombstoned, version) -> {
                if (timestamp != -1) {
                    walValue[0] = new WALValue(rowType, value, timestamp, tombstoned, version);
                }
                return true;
            });
        return walValue[0];
    }

    public boolean get(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueStream stream) throws Exception {

        acquireOne();
        try {
            PartitionDelta partitionDelta = getPartitionDelta(versionedPartitionName);
            return storage.streamValues(prefix,
                storageStream -> partitionDelta.get(prefix, keys, (fp, rowType, _prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    if (valueTimestamp != -1) {
                        return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
                    } else {
                        return storageStream.stream(key);
                    }
                }),
                (rowType, _prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
                });
        } finally {
            releaseOne();
        }
    }

    public boolean containsKeys(VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyContainedStream stream) throws Exception {
        acquireOne();
        try {
            return storage.containsKeys(prefix,
                storageKeyStream -> getPartitionDelta(versionedPartitionName).containsKeys(prefix, keys,
                (_prefix, key, tombstoned, exists) -> {
                    if (exists) {
                        return stream.stream(prefix, key, !tombstoned);
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
        KeyValuePointerStream stream) throws Exception {

        return storage.streamPointers((storageStream) -> {
            PartitionDelta partitionDelta = getPartitionDelta(versionedPartitionName);
            return partitionDelta.getPointers(keyValues,
                (rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion,
                    pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp) -> {
                    if (pointerFp == -1) {
                        return storageStream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
                    } else {
                        return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion,
                            pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp);
                    }
                });
        }, (rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp) -> {
            if (pointerFp == -1) {
                return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, -1, false, -1, -1);
            } else {
                return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion,
                    pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp);
            }
        });

    }

    public boolean rangeScan(VersionedPartitionName versionedPartitionName,
        RangeScannable rangeScannable,
        byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        KeyValueStream keyValueStream) throws Exception {

        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            final DeltaPeekableElmoIterator iterator = delta.rangeScanIterator(fromPrefix, fromKey, toPrefix, toKey);
            rangeScannable.rangeScan(fromPrefix, fromKey, toPrefix, toKey, new LatestKeyValueStream(iterator, keyValueStream));

            return WALKey.decompose(
                fpRawKeyValueStream -> {
                    Map.Entry<byte[], WALValue> d = iterator.last();
                    if (d != null || iterator.hasNext()) {
                        if (d != null) {
                            WALValue got = d.getValue();
                            if (!fpRawKeyValueStream.stream(-1, -1, got.getRowType(), d.getKey(),
                                got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
                                return false;
                            }
                        }
                        while (iterator.hasNext()) {
                            d = iterator.next();
                            WALValue got = d.getValue();
                            if (!fpRawKeyValueStream.stream(-1, -1, got.getRowType(), d.getKey(),
                                got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row)
                -> keyValueStream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion));
        } finally {
            releaseOne();
        }
    }

    public boolean rowScan(final VersionedPartitionName versionedPartitionName, Scannable scannable, KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            PartitionDelta delta = getPartitionDelta(versionedPartitionName);
            DeltaPeekableElmoIterator iterator = delta.rowScanIterator();
            if (!scannable.rowScan(new LatestKeyValueStream(iterator, keyValueStream))) {
                return false;
            }

            Map.Entry<byte[], WALValue> d = iterator.last();
            if (d != null || iterator.hasNext()) {
                return WALKey.decompose(
                    fpRawKeyValueStream -> {
                        Map.Entry<byte[], WALValue> last = d;
                        if (last != null) {
                            WALValue got = last.getValue();
                            if (!fpRawKeyValueStream.stream(-1, -1, got.getRowType(), last.getKey(),
                                got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
                                return false;
                            }
                        }
                        while (iterator.hasNext()) {
                            last = iterator.next();
                            WALValue got = last.getValue();
                            if (!fpRawKeyValueStream.stream(-1, -1, got.getRowType(), last.getKey(),
                                got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
                                return false;
                            }
                        }
                        return true;
                    },
                    (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row)
                    -> keyValueStream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion));
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
        acquireOne();
        try {
            return storage.count(stream -> getPartitionDelta(versionedPartitionName).keys(stream::stream));
        } finally {
            releaseOne();
        }
    }

    static class LatestKeyValueStream implements KeyValueStream {

        private final DeltaPeekableElmoIterator iterator;
        private final KeyValueStream keyValueStream;
        private Map.Entry<byte[], WALValue> d;

        public LatestKeyValueStream(DeltaPeekableElmoIterator iterator, KeyValueStream keyValueStream) {
            this.iterator = iterator;
            this.keyValueStream = keyValueStream;
        }

        @Override
        public boolean stream(RowType rowType,
            byte[] prefix,
            byte[] key,
            byte[] value,
            long valueTimestamp,
            boolean valueTombstoned,
            long valueVersion) throws Exception {
            if (d == null && iterator.hasNext()) {
                d = iterator.next();
            }
            boolean[] needsKey = {true};
            byte[] pk = WALKey.compose(prefix, key);
            boolean complete = WALKey.decompose(
                txFpKeyValueStream -> {
                    while (d != null && KeyUtil.compare(d.getKey(), pk) <= 0) {
                        WALValue got = d.getValue();
                        if (Arrays.equals(d.getKey(), pk)) {
                            needsKey[0] = false;
                        }
                        if (!txFpKeyValueStream.stream(-1, -1, got.getRowType(), d.getKey(),
                            got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
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
                    return true;
                },
                (txId, fp, rowType2, streamPrefix, streamKey, streamValue, streamValueTimestamp, streamValueTombstoned, streamValueVersion, row)
                -> keyValueStream.stream(rowType2, streamPrefix, streamKey, streamValue, streamValueTimestamp, streamValueTombstoned, streamValueVersion));

            if (!complete) {
                return false;
            } else if (needsKey[0]) {
                return keyValueStream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
            } else {
                return true;
            }
        }
    }

}
