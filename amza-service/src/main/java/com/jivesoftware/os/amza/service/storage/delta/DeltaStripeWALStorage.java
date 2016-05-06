package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.RangeScannable;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.scan.Scannable;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyValues;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.api.wal.KeyedTimestampId;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALPointer;
import com.jivesoftware.os.amza.api.wal.WALTimestampId;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.amza.service.AckWaters;
import com.jivesoftware.os.amza.service.NotARingMemberException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.CurrentVersionProvider;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionFamily;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.delta.DeltaWAL.KeyValueHighwater;
import com.jivesoftware.os.amza.service.storage.delta.PartitionDelta.MergeResult;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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

    private final BAInterner interner;
    private final int index;
    private final AmzaStats amzaStats;
    private final AckWaters ackWaters;
    private final SickThreads sickThreads;
    private final AmzaRingReader ringReader;
    private final DeltaWALFactory deltaWALFactory;
    private final WALIndexProviderRegistry walIndexProviderRegistry;
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

    public DeltaStripeWALStorage(BAInterner interner,
        int index,
        AmzaStats amzaStats,
        AckWaters ackWaters,
        SickThreads sickThreads,
        AmzaRingReader ringReader,
        DeltaWALFactory deltaWALFactory,
        WALIndexProviderRegistry walIndexProviderRegistry,
        long mergeAfterNUpdates,
        int mergeDeltaThreads) {

        this.interner = interner;
        this.index = index;
        this.amzaStats = amzaStats;
        this.ackWaters = ackWaters;
        this.sickThreads = sickThreads;
        this.ringReader = ringReader;
        this.deltaWALFactory = deltaWALFactory;
        this.walIndexProviderRegistry = walIndexProviderRegistry;
        this.mergeAfterNUpdates = mergeAfterNUpdates;
        this.mergeDeltaThreads = Executors.newFixedThreadPool(mergeDeltaThreads,
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
            reentrant.remove();
        } else {
            reentrant.set(enters - 1);
        }
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

    public void delete(VersionedPartitionName versionedPartitionName) throws Exception {
        acquireAll();
        try {
            partitionDeltas.remove(versionedPartitionName);
        } finally {
            releaseAll();
        }
    }

    public void load(PartitionIndex partitionIndex,
        VersionedPartitionProvider versionedPartitionProvider,
        CurrentVersionProvider currentVersionProvider,
        PrimaryRowMarshaller primaryRowMarshaller) throws Exception {

        LOG.info("Reloading deltas...");
        long start = System.currentTimeMillis();
        synchronized (oneWriterAtATimeLock) {
            List<DeltaWAL> deltaWALs = deltaWALFactory.list();
            if (deltaWALs.isEmpty()) {
                deltaWAL.set(deltaWALFactory.create(-1));
            } else {
                for (int i = 0; i < deltaWALs.size(); i++) {
                    DeltaWAL prevWAL = deltaWAL.get();
                    DeltaWAL currentWAL = deltaWALs.get(i);
                    if (prevWAL != null) {
                        Preconditions.checkState(currentWAL.getPrevId() == prevWAL.getId(), "Delta WALs were not contiguous");
                        mergeDelta(partitionIndex, versionedPartitionProvider, currentVersionProvider, prevWAL, true, () -> currentWAL);
                    }
                    deltaWAL.set(currentWAL);
                    Set<VersionedPartitionName> accepted = Sets.newHashSet();
                    Set<VersionedPartitionName> rejected = Sets.newHashSet();
                    WALKey.decompose(
                        (WALKey.TxFpRawKeyValueEntries<VersionedPartitionName>) txRawKeyEntryStream -> primaryRowMarshaller.fromRows(
                            txFpRowStream -> {
                                currentWAL.load((rowFP, rowTxId, rowType, rawRow) -> {
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
                                VersionedPartitionName versionedPartitionName = VersionedPartitionName.fromBytes(prefix, 0, interner);
                                try {
                                    boolean acceptable;
                                    if (accepted.contains(versionedPartitionName)) {
                                        acceptable = true;
                                    } else if (rejected.contains(versionedPartitionName)) {
                                        acceptable = false;
                                    } else {
                                        acceptable = currentVersionProvider.isCurrentVersion(versionedPartitionName);
                                        if (acceptable) {
                                            accepted.add(versionedPartitionName);
                                        } else {
                                            rejected.add(versionedPartitionName);
                                        }
                                    }
                                    return !acceptable || txRawKeyEntryStream.stream(rowTxId, rowFP, rowType, key,
                                        value, valueTimestamp, valueTombstoned, valueVersion, versionedPartitionName);
                                } catch (PropertiesNotPresentException e) {
                                    LOG.warn("Properties not available on load for {}", versionedPartitionName);
                                    return true;
                                } catch (NotARingMemberException e) {
                                    LOG.warn("Not a ring member for {}", versionedPartitionName);
                                    return true;
                                }
                            }),
                        (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, versionedPartitionName) -> {
                            acquireOne();
                            try {
                                PartitionDelta delta = getPartitionDelta(versionedPartitionName);
                                // delta is pristine, no need to check timestamps and versions
                                delta.put(fp, prefix, key, valueTimestamp, valueTombstoned, valueVersion);
                                delta.onLoadAppendTxFp(prefix, txId, fp);
                                updateSinceLastMerge.incrementAndGet();
                                return true;
                            } finally {
                                releaseOne();
                            }
                        });
                }
            }
        }

        amzaStats.deltaStripeLoad(index, updateSinceLastMerge.get(), updateSinceLastMerge.get() / (double) mergeAfterNUpdates);

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

    public boolean hasChangesFor(VersionedPartitionName versionedPartitionName) {
        return partitionDeltas.containsKey(versionedPartitionName);
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
            partitionDelta = partitionDeltas.computeIfAbsent(versionedPartitionName, (t) -> new PartitionDelta(versionedPartitionName, wal, null));
        }
        return partitionDelta;
    }

    public boolean mergeable() {
        return updateSinceLastMerge.get() > mergeAfterNUpdates;
    }

    public void merge(PartitionIndex partitionIndex,
        VersionedPartitionProvider versionedPartitionProvider,
        CurrentVersionProvider currentVersionProvider,
        boolean force) throws Exception {

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
            DeltaWAL wal = deltaWAL.get();
            if (mergeDelta(partitionIndex, versionedPartitionProvider, currentVersionProvider, wal, false, () -> deltaWALFactory.create(wal.getId()))) {
                merging.set(0);
            }
        } finally {
            amzaStats.endCompaction(CompactionFamily.merge, "Delta Stripe:" + index);
        }
    }

    private boolean mergeDelta(
        PartitionIndex partitionIndex,
        VersionedPartitionProvider versionedPartitionProvider,
        CurrentVersionProvider currentVersionProvider,
        DeltaWAL wal,
        boolean validate,
        Callable<DeltaWAL> newWAL) throws Exception {
        final List<Future<MergeResult>> futures = new ArrayList<>();
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
            amzaStats.deltaStripeLoad(index, 0, 0);

            AtomicLong mergeable = new AtomicLong();
            AtomicLong merged = new AtomicLong();
            AtomicLong unmerged = new AtomicLong();

            Iterator<Entry<VersionedPartitionName, PartitionDelta>> iter = partitionDeltas.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<VersionedPartitionName, PartitionDelta> entry = iter.next();
                VersionedPartitionName versionedPartitionName = entry.getKey();

                if (currentVersionProvider.isCurrentVersion(versionedPartitionName)) {
                    PartitionDelta mergeableDelta = entry.getValue();
                    long mergeableCount = mergeableDelta.size();
                    unmerged.addAndGet(mergeableCount);
                    PartitionDelta currentDelta = new PartitionDelta(versionedPartitionName, newDeltaWAL, mergeableDelta);
                    entry.setValue(currentDelta);
                    mergeable.incrementAndGet();
                    futures.add(mergeDeltaThreads.submit(() -> {
                        MergeResult result = null;
                        try {
                            while (true) {
                                try {
                                    PartitionName partitionName = versionedPartitionName.getPartitionName();
                                    result = currentVersionProvider.tx(partitionName,
                                        null,
                                        (deltaIndex, stripeIndex, storageVersion) -> {
                                            MergeResult r;
                                            if (stripeIndex == -1) {
                                                LOG.warn("Ignored merge for partition {} with nonexistent storage", versionedPartitionName);
                                                r = null;
                                            } else {
                                                PartitionProperties properties = versionedPartitionProvider.getProperties(partitionName);
                                                if (properties == null) {
                                                    LOG.warn("Ignored merge for partition {} with missing properties", versionedPartitionName);
                                                    r = null;
                                                } else {
                                                    r = currentDelta.merge(partitionIndex, properties, stripeIndex, validate);
                                                }
                                            }
                                            sickThreads.recovered();
                                            return r;
                                        });

                                    break;
                                } catch (Throwable x) {
                                    sickThreads.sick(x);
                                    if (validate) {
                                        LOG.error("Validation merge failed for partition:{} WAL storage must be purged and re-taken!",
                                            new Object[] { versionedPartitionName }, x);
                                        currentVersionProvider.abandonVersion(versionedPartitionName);
                                        break;
                                    } else {
                                        LOG.error("Background merge failed for partition:{} We will retry in case the issue can be resolved.",
                                            new Object[] { versionedPartitionName }, x);
                                        Thread.sleep(30_000L);
                                    }
                                }
                            }
                            return result;
                        } finally {
                            amzaStats.deltaStripeMerge(index,
                                mergeable.decrementAndGet(),
                                (unmerged.get() - merged.addAndGet(mergeableCount)) / (double) unmerged.get());
                            sickThreads.recovered();
                        }
                    }));
                } else {
                    LOG.warn("Ignored merge for obsolete partition {}", versionedPartitionName);
                    iter.remove();
                }
            }
            amzaStats.deltaStripeMerge(index, 0, 0);
        } catch (Exception x) {
            sickThreads.sick(x);
            LOG.error(
                "This is catastrophic."
                    + " We have permanently parked this thread."
                    + " This delta {} can no longer accept writes."
                    + " You likely need to restart this instance",
                new Object[] { index }, x);
            LockSupport.park();
        } finally {
            releaseAll();
        }

        List<MergeResult> results = Lists.newArrayListWithCapacity(futures.size());
        for (Future<MergeResult> f : futures) {
            MergeResult result = f.get();
            if (result != null) {
                results.add(result);
            }
        }
        ListMultimap<String, WALIndex> providerIndexes = ArrayListMultimap.create();
        for (MergeResult result : results) {
            if (result.partitionStore != null) {
                result.partitionStore.flush(true);
            }
            if (result.walIndex != null) {
                providerIndexes.put(result.walIndex.getProviderName(), result.walIndex);
            }
        }
        for (Entry<String, Collection<WALIndex>> entry : providerIndexes.asMap().entrySet()) {
            WALIndexProvider<?> walIndexProvider = walIndexProviderRegistry.getWALIndexProvider(entry.getKey());
            if (walIndexProvider != null) {
                walIndexProvider.flush((Iterable) entry.getValue(), true);
            }
        }
        acquireAll();
        try {
            deltaWALFactory.destroy(wal);
            LOG.info("Compacted delta partitions.");
        } finally {
            releaseAll();
        }

        for (MergeResult result : results) {
            currentVersionProvider.invalidateDeltaIndexCache(result.versionedPartitionName);
        }
        return true;
    }

    public RowsChanged update(boolean directApply,
        RowType rowType,
        HighwaterStorage highwaterStorage,
        VersionedPartitionName versionedPartitionName,
        PartitionStore partitionStore,
        byte[] prefix,
        Commitable updates,
        WALUpdated updated) throws Exception {

        long mergeDebt = merging.get();
        if ((mergeDebt > 0 && mergeDebt + updateSinceLastMerge.get() > (2 * mergeAfterNUpdates))
            || updateSinceLastMerge.get() > (2 * mergeAfterNUpdates)) {
            throw new DeltaOverCapacityException("Delta is full");
        }

        if (directApply && mergeDebt > 0) {
            long highestTxId = partitionStore.mergedTxId();
            int takeFromFactor = ringReader.getTakeFromFactor(versionedPartitionName.getPartitionName().getRingName());
            int[] taken = { 0 };
            ackWaters.streamPartitionTxIds(versionedPartitionName, (member, txId) -> {
                if (txId >= highestTxId) {
                    taken[0]++;
                    if (taken[0] >= takeFromFactor) {
                        return false;
                    }
                }
                return true;
            });
            if (taken[0] < takeFromFactor) {
                throw new DeltaOverCapacityException("Delta requires replication");
            }
        }

        final Map<WALKey, WALValue> apply = new LinkedHashMap<>();

        final List<KeyedTimestampId> removes = new ArrayList<>();
        final List<KeyedTimestampId> clobbers = new ArrayList<>();

        final List<byte[]> keys = new ArrayList<>();
        final List<WALValue> values = new ArrayList<>();
        updates.commitable(null, (transactionId, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
            Preconditions.checkArgument(valueTimestamp > 0, "Timestamp must be greater than zero");
            Preconditions.checkArgument(valueVersion > 0, "Timestamp must be greater than zero");
            keys.add(key);
            values.add(new WALValue(rowType, value, valueTimestamp, valueTombstone, valueVersion));
            return true;
        });

        acquireOne();
        try {
            DeltaWAL wal = deltaWAL.get();
            RowsChanged rowsChanged;

            getPointers(versionedPartitionName,
                partitionStore.getWalStorage(),
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
                    Iterator<Entry<WALKey, WALValue>> iter = apply.entrySet().iterator();
                    while (iter.hasNext()) {
                        Entry<WALKey, WALValue> entry = iter.next();
                        WALKey key = entry.getKey();
                        WALValue value = entry.getValue();
                        WALPointer got = delta.getPointer(key.prefix, key.key);
                        if (got != null && CompareTimestampVersions.compare(got.getTimestampId(), got.getVersion(),
                            value.getTimestampId(), value.getVersion()) >= 0) {
                            iter.remove();
                            amzaStats.deltaSecondCheckRemoves.incrementAndGet();
                        }
                    }

                    updateApplied = wal.update(rowType, versionedPartitionName, apply, partitionHighwater);
                    appliedCount = apply.size();

                    for (int i = 0; i < updateApplied.fps.length; i++) {
                        KeyValueHighwater keyValueHighwater = updateApplied.keyValueHighwaters[i];
                        long fp = updateApplied.fps[i];
                        delta.put(fp,
                            keyValueHighwater.prefix,
                            keyValueHighwater.key,
                            keyValueHighwater.valueTimestamp,
                            keyValueHighwater.valueTombstone,
                            keyValueHighwater.valueVersion);
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
            amzaStats.deltaStripeLoad(index, unmergedUpdates, unmergedUpdates / (double) mergeAfterNUpdates);
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
            boolean[] needsKey = { true };
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

    @Override
    public String toString() {
        return "DeltaStripeWALStorage{" + "index=" + index + '}';
    }

}
