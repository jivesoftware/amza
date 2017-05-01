package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.IoStats;
import com.jivesoftware.os.amza.api.filer.UIO;
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
import com.jivesoftware.os.amza.api.wal.WALCompactionStats;
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
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionStats;
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
import java.util.concurrent.ExecutorService;
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

    private final AmzaInterner amzaInterner;
    private final int index;
    private final AmzaStats amzaStats;
    private final AckWaters ackWaters;
    private final SickThreads sickThreads;
    private final AmzaRingReader ringReader;
    private final HighwaterStorage highwaterStorage;
    private final DeltaWALFactory deltaWALFactory;
    private final int maxValueSizeInIndex;
    private final boolean useHighwaterTxId;
    private final WALIndexProviderRegistry walIndexProviderRegistry;
    private final long mergeAfterNUpdates;
    private final ExecutorService mergeDeltaThreads;

    private final Object awakeCompactionsLock = new Object();
    private final AtomicReference<DeltaWAL> deltaWAL = new AtomicReference<>();
    private final Map<VersionedPartitionName, PartitionDelta> partitionDeltas = Maps.newConcurrentMap();
    private final Object oneWriterAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
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

    public DeltaStripeWALStorage(AmzaInterner amzaInterner,
        int index,
        AmzaStats amzaStats,
        AckWaters ackWaters,
        SickThreads sickThreads,
        AmzaRingReader ringReader,
        HighwaterStorage highwaterStorage,
        DeltaWALFactory deltaWALFactory,
        int maxValueSizeInIndex,
        boolean useHighwaterTxId,
        WALIndexProviderRegistry walIndexProviderRegistry,
        long mergeAfterNUpdates,
        ExecutorService mergeDeltaThreads) {

        this.amzaInterner = amzaInterner;
        this.index = index;
        this.amzaStats = amzaStats;
        this.ackWaters = ackWaters;
        this.sickThreads = sickThreads;
        this.ringReader = ringReader;
        this.highwaterStorage = highwaterStorage;
        this.deltaWALFactory = deltaWALFactory;
        this.maxValueSizeInIndex = maxValueSizeInIndex;
        this.useHighwaterTxId = useHighwaterTxId;
        this.walIndexProviderRegistry = walIndexProviderRegistry;
        this.mergeAfterNUpdates = mergeAfterNUpdates;
        this.mergeDeltaThreads = mergeDeltaThreads;
    }

    public int getId() {
        return index;
    }

    private void acquireOne() throws InterruptedException {
    }

    private void releaseOne() {
    }

    private void writeAcquireOne() throws InterruptedException {
        int enters = reentrant.get();
        if (enters == 0) {
            tickleMeElmophore.acquire();
        }
        reentrant.set(enters + 1);
    }

    private void writeReleaseOne() {
        int enters = reentrant.get();
        if (enters - 1 == 0) {
            tickleMeElmophore.release();
            reentrant.remove();
        } else {
            reentrant.set(enters - 1);
        }
    }

    private void writeAcquireAll() throws InterruptedException {
        tickleMeElmophore.acquire(numTickleMeElmaphore);
    }

    private void writeReleaseAll() {
        tickleMeElmophore.release(numTickleMeElmaphore);
    }

    public Object getAwakeCompactionLock() {
        return awakeCompactionsLock;
    }

    public void delete(VersionedPartitionName versionedPartitionName) throws Exception {
        writeAcquireAll();
        try {
            synchronized (partitionDeltas) {
                partitionDeltas.remove(versionedPartitionName);
            }
        } finally {
            writeReleaseAll();
        }
    }

    public void load(IoStats ioStats, PartitionIndex partitionIndex,
        VersionedPartitionProvider versionedPartitionProvider,
        CurrentVersionProvider currentVersionProvider,
        PrimaryRowMarshaller primaryRowMarshaller) throws Exception {

        LOG.info("Reloading deltas...");
        long start = System.currentTimeMillis();
        CompactionStats compactionStats = amzaStats.beginCompaction(CompactionFamily.load, "load-delta-stripe-" + getId());
        try {
            synchronized (oneWriterAtATimeLock) {
                List<DeltaWAL> deltaWALs = deltaWALFactory.list(ioStats);
                if (deltaWALs.isEmpty()) {
                    deltaWAL.set(deltaWALFactory.create(ioStats, -1));
                } else {
                    for (int i = 0; i < deltaWALs.size(); i++) {
                        DeltaWAL prevWAL = deltaWAL.get();
                        DeltaWAL currentWAL = deltaWALs.get(i);
                        if (prevWAL != null) {
                            Preconditions.checkState(currentWAL.getPrevId() == prevWAL.getId(),
                                "Delta WALs were not contiguous, %s->%s", currentWAL.getPrevId(), prevWAL.getId());
                            mergeDelta(ioStats, compactionStats, partitionIndex, versionedPartitionProvider, currentVersionProvider, prevWAL, true,
                                () -> currentWAL);
                        }
                        deltaWAL.set(currentWAL);
                        Set<VersionedPartitionName> accepted = Sets.newHashSet();
                        Set<VersionedPartitionName> rejected = Sets.newHashSet();
                        WALKey.decompose(
                            (WALKey.TxFpRawKeyValueEntries<VersionedPartitionName>) txRawKeyEntryStream -> primaryRowMarshaller.fromRows(
                                txFpRowStream -> {
                                    currentWAL.load(ioStats, (rowFP, rowTxId, rowType, rawRow) -> {
                                        if (rowType.isPrimary()) {
                                            if (!txFpRowStream.stream(rowTxId, rowFP, rowType, rawRow)) {
                                                return false;
                                            }
                                        }
                                        return true;
                                    });
                                    return true;
                                },
                                (rowTxId, rowFP, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, row) -> {
                                    VersionedPartitionName versionedPartitionName = amzaInterner.internVersionedPartitionName(prefix, 0, prefix.length);
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
                                            hasValue, value, valueTimestamp, valueTombstoned, valueVersion, versionedPartitionName);
                                    } catch (PropertiesNotPresentException e) {
                                        LOG.warn("Properties not available on load for {}", versionedPartitionName);
                                        return true;
                                    } catch (NotARingMemberException e) {
                                        LOG.warn("Not a ring member for {}", versionedPartitionName);
                                        return true;
                                    }
                                }),
                            (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, versionedPartitionName) -> {
                                acquireOne();
                                try {
                                    return txPartitionDelta(versionedPartitionName, delta -> {
                                        // delta is pristine, no need to check timestamps and versions
                                        byte[] deltaValue = UIO.readByteArray(value, 0, "value");
                                        delta.put(fp, prefix, key, deltaValue, valueTimestamp, valueTombstoned, valueVersion);
                                        delta.onLoadAppendTxFp(prefix, txId, fp);
                                        updateSinceLastMerge.incrementAndGet();
                                        return true;
                                    });
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
        } finally {
            compactionStats.finished();
        }
    }

    private interface PartitionDeltaTx {
        boolean tx(PartitionDelta delta) throws Exception;
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

    public interface StorageTxIdProvider {
        long get() throws Exception;
    }

    public long getHighestTxId(VersionedPartitionName versionedPartitionName, StorageTxIdProvider txIdProvider) throws Exception {
        PartitionDelta partitionDelta = partitionDeltas.get(versionedPartitionName);
        if (partitionDelta != null) {
            long highestTxId = partitionDelta.highestTxId();
            if (highestTxId > -1) {
                return highestTxId;
            }
        }
        if (useHighwaterTxId) {
            long highwaterTxId = highwaterStorage.getLocal(versionedPartitionName);
            if (highwaterTxId == HighwaterStorage.LOCAL_NONE) {
                highwaterTxId = txIdProvider.get();
                if (highwaterTxId != HighwaterStorage.LOCAL_NONE) {
                    LOG.info("Repaired missing highwater for:{} txId:{}", versionedPartitionName, highwaterTxId);
                    highwaterStorage.setLocal(versionedPartitionName, highwaterTxId);
                }
            }
            return highwaterTxId;
        } else {
            long highwaterTxId = highwaterStorage.getLocal(versionedPartitionName);
            long storageTxId = txIdProvider.get();
            if (highwaterTxId == HighwaterStorage.LOCAL_NONE && storageTxId != HighwaterStorage.LOCAL_NONE) {
                LOG.info("Repaired missing highwater for:{} txId:{}", versionedPartitionName, storageTxId);
                highwaterStorage.setLocal(versionedPartitionName, storageTxId);
            } else if (highwaterTxId != HighwaterStorage.LOCAL_NONE && storageTxId > highwaterTxId) {
                LOG.error("Lagging txId for:{} storage:{} highwater:{}", versionedPartitionName, storageTxId, highwaterTxId);
            }
            return storageTxId;
        }
    }

    private boolean txPartitionDelta(VersionedPartitionName versionedPartitionName, PartitionDeltaTx tx) throws Exception {
        PartitionDelta partitionDelta;
        synchronized (partitionDeltas) {
            partitionDelta = partitionDeltas.get(versionedPartitionName);
            if (partitionDelta == null) {
                DeltaWAL wal = deltaWAL.get();
                if (wal == null) {
                    throw new IllegalStateException("Delta WAL is currently unavailable.");
                }
                partitionDelta = partitionDeltas.computeIfAbsent(versionedPartitionName,
                    vpn -> new PartitionDelta(versionedPartitionName, wal, maxValueSizeInIndex, null));
            }
            partitionDelta.acquire();
        }
        try {
            return tx.tx(partitionDelta);
        } finally {
            partitionDelta.release();
        }
    }

    public boolean mergeable() {
        return updateSinceLastMerge.get() > mergeAfterNUpdates;
    }

    public void merge(IoStats ioStats,
        PartitionIndex partitionIndex,
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
        CompactionStats compactionStats = amzaStats.beginCompaction(CompactionFamily.merge, "merge-delta-stripe" + getId());
        try {
            DeltaWAL wal = deltaWAL.get();
            updateSinceLastMerge.set(0);
            boolean mergeDelta = mergeDelta(ioStats,
                compactionStats,
                partitionIndex,
                versionedPartitionProvider,
                currentVersionProvider,
                wal,
                false,
                () -> deltaWALFactory.create(ioStats, wal.getId())
            );
            if (!mergeDelta) {
                updateSinceLastMerge.addAndGet(had);
            }
            merging.set(0);
        } finally {
            compactionStats.finished();
        }
    }

    private boolean mergeDelta(
        IoStats ioStats,
        WALCompactionStats walCompactionStats,
        PartitionIndex partitionIndex,
        VersionedPartitionProvider versionedPartitionProvider,
        CurrentVersionProvider currentVersionProvider,
        DeltaWAL wal,
        boolean validate,
        Callable<DeltaWAL> newWAL) throws Exception {

        List<Future<MergeResult>> futures = new ArrayList<>();
        writeAcquireAll();
        try {
            synchronized (partitionDeltas) {
                for (Map.Entry<VersionedPartitionName, PartitionDelta> e : partitionDeltas.entrySet()) {
                    if (e.getValue().isMerging()) {
                        LOG.warn("Ingress is faster than we can merge!");
                        return false;
                    }
                }
                LOG.info("Merging delta partitions...");
                DeltaWAL newDeltaWAL = newWAL.call();
                deltaWAL.set(newDeltaWAL);
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
                        if (mergeableDelta.needsToMerge()) {
                            long mergeableCount = mergeableDelta.size();
                            unmerged.addAndGet(mergeableCount);
                            PartitionDelta currentDelta = new PartitionDelta(versionedPartitionName, newDeltaWAL, maxValueSizeInIndex, mergeableDelta);
                            entry.setValue(currentDelta);
                            mergeable.incrementAndGet();
                            futures.add(mergeDeltaThreads.submit(() -> {
                                return getMergeResult(ioStats,
                                    walCompactionStats,
                                    partitionIndex,
                                    versionedPartitionProvider,
                                    currentVersionProvider,
                                    validate,
                                    mergeable,
                                    merged,
                                    unmerged,
                                    versionedPartitionName,
                                    mergeableCount,
                                    currentDelta);
                            }));
                        } else {
                            LOG.warn("Ignored merge for empty partition {}", versionedPartitionName);
                            iter.remove();
                        }
                    } else {
                        LOG.warn("Ignored merge for obsolete partition {}", versionedPartitionName);
                        iter.remove();
                    }
                }
                amzaStats.deltaStripeMerge(index, 0, 0);
            }
        } catch (Exception x) {
            parkSick("This is catastrophic."
                + " We have permanently parked this thread."
                + " This delta {} can no longer accept writes."
                + " You likely need to restart this instance", x);
        } finally {
            writeReleaseAll();
        }

        List<MergeResult> results = Lists.newArrayListWithCapacity(futures.size());
        try {
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
                highwaterStorage.setLocal(result.versionedPartitionName, result.lastTxId);
            }
            for (Entry<String, Collection<WALIndex>> entry : providerIndexes.asMap().entrySet()) {
                WALIndexProvider<?> walIndexProvider = walIndexProviderRegistry.getWALIndexProvider(entry.getKey());
                if (walIndexProvider != null) {
                    walIndexProvider.flush((Iterable) entry.getValue(), true);
                }
            }
        } catch (Exception x) {
            parkSick("This is catastrophic. Failure finalizing merge.", x);
        }

        try {
            wal.awaitDerefenced();
            LOG.info("Awaited clear references for delta partitions.");
        } catch (Exception x) {
            parkSick("This is catastrophic. Failure awaiting clear references.", x);
        }

        try {
            highwaterStorage.flushLocal();
            highwaterStorage.flush(index, true, () -> {
                flush(true);
                return null;
            });
        } catch (Exception x) {
            parkSick("This is catastrophic. Failure flushing highwaters.", x);
        }

        try {
            deltaWALFactory.destroy(wal);
            LOG.info("Compacted delta partitions.");
        } catch (Exception x) {
            parkSick("This is catastrophic. Failure destroying WAL.", x);
        }

        try {
            for (MergeResult result : results) {
                currentVersionProvider.invalidateDeltaIndexCache(result.versionedPartitionName);
            }
        } catch (Exception x) {
            parkSick("This is catastrophic. Failure invalidating delta index cache.", x);
        }
        return true;
    }

    private void parkSick(String message, Exception x) {
        sickThreads.sick(x);
        LOG.error(message
                + " We have permanently parked this thread."
                + " This delta {} can no longer accept writes."
                + " You likely need to restart this instance",
            new Object[] { index }, x);
        LockSupport.park();
    }

    private MergeResult getMergeResult(IoStats ioStats,
        WALCompactionStats walCompactionStats,
        PartitionIndex partitionIndex,
        VersionedPartitionProvider versionedPartitionProvider,
        CurrentVersionProvider currentVersionProvider,
        boolean validate,
        AtomicLong mergeable,
        AtomicLong merged,
        AtomicLong unmerged,
        VersionedPartitionName versionedPartitionName,
        long mergeableCount,
        PartitionDelta currentDelta) throws Exception {
        MergeResult result = null;
        try {
            PartitionName partitionName = versionedPartitionName.getPartitionName();
            walCompactionStats.add("partitions", 1);
            walCompactionStats.start(partitionName.toBase64());
            try {
                while (true) {
                    try {
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
                                        r = currentDelta.merge(ioStats, partitionIndex, properties, stripeIndex, validate);
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
            } finally {
                walCompactionStats.stop(partitionName.toBase64());
            }
            return result;
        } finally {
            amzaStats.deltaStripeMerge(index,
                mergeable.decrementAndGet(),
                (unmerged.get() - merged.addAndGet(mergeableCount)) / (double) unmerged.get());
            sickThreads.recovered();
        }
    }

    public RowsChanged update(IoStats ioStats,
        boolean directApply,
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
            int takeFromFactor = ringReader.getTakeFromFactor(versionedPartitionName.getPartitionName().getRingName(), 0);
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

        Map<WALKey, WALValue> apply = new LinkedHashMap<>();

        List<KeyedTimestampId> removes = new ArrayList<>();
        List<KeyedTimestampId> clobbers = new ArrayList<>();

        List<byte[]> keys = new ArrayList<>();
        List<WALValue> values = new ArrayList<>();
        updates.commitable(null, (transactionId, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
            Preconditions.checkArgument(valueTimestamp > 0, "Timestamp must be greater than zero");
            Preconditions.checkArgument(valueVersion > 0, "Timestamp must be greater than zero");
            keys.add(key);
            values.add(new WALValue(rowType, value, valueTimestamp, valueTombstone, valueVersion));
            return true;
        });

        writeAcquireOne();
        try {
            DeltaWAL wal = deltaWAL.get();
            RowsChanged[] rowsChanged = { null };

            getPointers(ioStats,
                versionedPartitionName,
                partitionStore.getWalStorage(),
                (stream) -> {
                    for (int i = 0; i < keys.size(); i++) {
                        byte[] key = keys.get(i);
                        WALValue update = values.get(i);
                        if (!stream.stream(prefix, key, update.getValue(), update.getTimestampId(), update.getTombstoned(), update.getVersion())) {
                            return false;
                        }
                    }
                    return true;
                },
                (_prefix, key, value, valueTimestamp, valueTombstone, valueVersion, ptrTimestamp, ptrTombstoned, ptrVersion, ptrFp, ptrHasValue, ptrValue) -> {
                    WALKey walKey = new WALKey(prefix, key);
                    WALValue walValue = new WALValue(rowType, value, valueTimestamp, valueTombstone, valueVersion);
                    if (ptrFp == -1 && !ptrHasValue) {
                        apply.put(walKey, walValue);
                    } else if (CompareTimestampVersions.compare(ptrTimestamp, ptrVersion, valueTimestamp, valueVersion) < 0) {
                        apply.put(walKey, walValue);
                        WALTimestampId walTimestampId = new WALTimestampId(ptrTimestamp, ptrTombstoned);
                        KeyedTimestampId keyedTimestampId = new KeyedTimestampId(prefix, key, walTimestampId.getTimestampId(), walTimestampId.getTombstoned());
                        clobbers.add(keyedTimestampId);
                        if (valueTombstone && !ptrTombstoned) {
                            removes.add(keyedTimestampId);
                        }
                    } else {
                        amzaStats.deltaFirstCheckRemoves.increment();
                    }
                    return true;
                });

            long[] appliedCount = { 0 };
            if (apply.isEmpty()) {
                rowsChanged[0] = new RowsChanged(versionedPartitionName, Collections.emptyMap(), removes, clobbers, -1, -1, index);
            } else {
                txPartitionDelta(versionedPartitionName, delta -> {
                    WALHighwater partitionHighwater = null;
                    if (delta.shouldWriteHighwater()) {
                        partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName, false);
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
                                amzaStats.deltaSecondCheckRemoves.increment();
                            }
                        }

                        updateApplied = wal.update(ioStats, rowType, versionedPartitionName, apply, partitionHighwater);
                        appliedCount[0] = apply.size();

                        for (int i = 0; i < updateApplied.fps.length; i++) {
                            KeyValueHighwater keyValueHighwater = updateApplied.keyValueHighwaters[i];
                            long fp = updateApplied.fps[i];
                            delta.put(fp,
                                keyValueHighwater.prefix,
                                keyValueHighwater.key,
                                keyValueHighwater.value,
                                keyValueHighwater.valueTimestamp,
                                keyValueHighwater.valueTombstone,
                                keyValueHighwater.valueVersion);
                        }
                        delta.appendTxFps(prefix, updateApplied.txId, updateApplied.fps);
                        rowsChanged[0] = new RowsChanged(versionedPartitionName,
                            apply,
                            removes,
                            clobbers,
                            updateApplied.txId,
                            updateApplied.txId,
                            index);
                    }
                    updated.updated(versionedPartitionName, updateApplied.txId);
                    return true;
                });
            }

            long unmergedUpdates = updateSinceLastMerge.addAndGet(appliedCount[0]);
            amzaStats.deltaStripeLoad(index, unmergedUpdates, unmergedUpdates / (double) mergeAfterNUpdates);
            if (unmergedUpdates > mergeAfterNUpdates) {
                synchronized (awakeCompactionsLock) {
                    awakeCompactionsLock.notifyAll();
                }
            }
            return rowsChanged[0];
        } finally {
            writeReleaseOne();
        }
    }

    public boolean takeRowsFromTransactionId(IoStats ioStats,
        VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        long transactionId,
        RowStream rowStream) throws Exception {

        long[] lowestTxId = { -1 };
        acquireOne();
        try {
            txPartitionDelta(versionedPartitionName, delta -> {
                lowestTxId[0] = delta.lowestTxId();
                return true;
            });
        } finally {
            releaseOne();
        }

        if ((lowestTxId[0] == -1 || lowestTxId[0] > transactionId) && !storage.takeRowUpdatesSince(ioStats, transactionId, rowStream)) {
            return false;
        }

        acquireOne();
        try {
            return txPartitionDelta(versionedPartitionName, delta -> {
                return delta.takeRowsFromTransactionId(ioStats, transactionId, rowStream);
            });
        } finally {
            releaseOne();
        }
    }

    public boolean takeRowsFromTransactionId(IoStats ioStats, VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        byte[] prefix,
        long transactionId,
        RowStream rowStream) throws Exception {

        long[] lowestTxId = { -1 };
        acquireOne();
        try {
            txPartitionDelta(versionedPartitionName, delta -> {
                lowestTxId[0] = delta.lowestTxId(prefix);
                return true;
            });
        } finally {
            releaseOne();
        }

        if ((lowestTxId[0] == -1 || lowestTxId[0] > transactionId) && !storage.takeRowUpdatesSince(prefix, transactionId, rowStream)) {
            return false;
        }

        acquireOne();
        try {
            return txPartitionDelta(versionedPartitionName, delta -> {
                return delta.takeRowsFromTransactionId(ioStats, prefix, transactionId, rowStream);
            });
        } finally {
            releaseOne();
        }
    }

    public boolean takeAllRows(IoStats ioStats, VersionedPartitionName versionedPartitionName, WALStorage storage, RowStream rowStream) throws Exception {

        if (!storage.takeAllRows(ioStats, rowStream)) {
            return false;
        }

        acquireOne();
        try {
            return txPartitionDelta(versionedPartitionName, delta -> {
                return delta.takeRowsFromTransactionId(ioStats, 0, rowStream);
            });
        } finally {
            releaseOne();
        }
    }

    // for testing
    WALValue get(IoStats ioStats, VersionedPartitionName versionedPartitionName, WALStorage storage, byte[] prefix, byte[] key) throws Exception {
        WALValue[] walValue = new WALValue[1];
        get(ioStats,
            versionedPartitionName,
            storage,
            prefix,
            stream -> stream.stream(key),
            (_prefix, _key, value, timestamp, tombstoned, version) -> {
                if (timestamp != -1) {
                    walValue[0] = new WALValue(null, value, timestamp, tombstoned, version);
                }
                return true;
            });
        return walValue[0];
    }

    public boolean get(IoStats ioStats, VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueStream stream) throws Exception {

        acquireOne();
        try {
            return txPartitionDelta(versionedPartitionName, partitionDelta -> {
                return storage.streamValues(prefix,
                    storageStream -> partitionDelta.get(ioStats, prefix, keys,
                        (fp, rowType, _prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                            if (valueTimestamp != -1) {
                                return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
                            } else {
                                return storageStream.stream(key);
                            }
                        }),
                    (_prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                        return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
                    });
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
                storageKeyStream -> txPartitionDelta(versionedPartitionName,
                    delta -> delta.containsKeys(prefix, keys,
                        (_prefix, key, timestamp, tombstoned, version, exists) -> {
                            if (exists) {
                                return stream.stream(prefix, key, !tombstoned, timestamp, version);
                            } else {
                                return storageKeyStream.stream(key);
                            }
                        })),
                stream);
        } finally {
            releaseOne();
        }
    }

    private boolean getPointers(IoStats ioStats,
        VersionedPartitionName versionedPartitionName,
        WALStorage storage,
        KeyValues keyValues,
        KeyValuePointerStream stream) throws Exception {

        return storage.streamPointers(
            ioStats,
            (storageStream) -> {
                return txPartitionDelta(versionedPartitionName,
                    partitionDelta -> partitionDelta.getPointers(keyValues,
                        (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, pTimestamp, pTombstoned, pVersion, pFp, pHasValue, pValue) -> {
                            if (pFp == -1 && !pHasValue) {
                                return storageStream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
                            } else {
                                return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion,
                                    pTimestamp, pTombstoned, pVersion, pFp, pHasValue, pValue);
                            }
                        }));
            },
            (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, ptrTimestamp, ptrTombstoned, ptrVersion, ptrFp, ptrHasValue, ptrValue) -> {
                if (ptrFp == -1 && !ptrHasValue) {
                    return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, -1, false, -1, -1, false, null);
                } else {
                    return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion,
                        ptrTimestamp, ptrTombstoned, ptrVersion, ptrFp, ptrHasValue, ptrValue);
                }
            });

    }

    public boolean rangeScan(VersionedPartitionName versionedPartitionName,
        RangeScannable rangeScannable,
        byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        KeyValueStream keyValueStream,
        boolean hydrateValues) throws Exception {

        acquireOne();
        try {
            return txPartitionDelta(versionedPartitionName, delta -> {
                final DeltaPeekableElmoIterator iterator = delta.rangeScanIterator(fromPrefix, fromKey, toPrefix, toKey, hydrateValues);
                try {
                    rangeScannable.rangeScan(fromPrefix, fromKey, toPrefix, toKey, new LatestKeyValueStream(iterator, keyValueStream), hydrateValues);
                    return WALKey.decompose(
                        fpRawKeyValueStream -> {
                            Map.Entry<byte[], WALValue> d = iterator.last();
                            if (d != null || iterator.hasNext()) {
                                if (d != null) {
                                    WALValue got = d.getValue();
                                    if (!fpRawKeyValueStream.stream(-1, -1, got.getRowType(), d.getKey(),
                                        true, got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
                                        return false;
                                    }
                                }
                                while (iterator.hasNext()) {
                                    d = iterator.next();
                                    WALValue got = d.getValue();
                                    if (!fpRawKeyValueStream.stream(-1, -1, got.getRowType(), d.getKey(),
                                        true, got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
                                        return false;
                                    }
                                }
                            }
                            return true;
                        },
                        (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, row)
                            -> keyValueStream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion));
                } finally {
                    iterator.close();
                }
            });
        } finally {
            releaseOne();
        }
    }

    public boolean rowScan(VersionedPartitionName versionedPartitionName,
        Scannable scannable,
        KeyValueStream keyValueStream,
        boolean hydrateValues) throws Exception {

        acquireOne();
        try {
            return txPartitionDelta(versionedPartitionName, delta -> {
                DeltaPeekableElmoIterator iterator = delta.rowScanIterator(hydrateValues);
                try {
                    if (!scannable.rowScan(new LatestKeyValueStream(iterator, keyValueStream), hydrateValues)) {
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
                                        true, got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
                                        return false;
                                    }
                                }
                                while (iterator.hasNext()) {
                                    last = iterator.next();
                                    WALValue got = last.getValue();
                                    if (!fpRawKeyValueStream.stream(-1, -1, got.getRowType(), last.getKey(),
                                        true, got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
                                        return false;
                                    }
                                }
                                return true;
                            },
                            (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, row)
                                -> keyValueStream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion));
                    }
                } finally {
                    iterator.close();
                }
                return true;
            });
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
            return storage.count(stream -> txPartitionDelta(versionedPartitionName, delta -> delta.keys(stream)));
        } finally {
            releaseOne();
        }
    }

    public long approximateCount(VersionedPartitionName versionedPartitionName, WALStorage storage) throws Exception {
        acquireOne();
        try {
            long[] deltaSize = { -1 };
            txPartitionDelta(versionedPartitionName, delta -> {
                deltaSize[0] = delta.size();
                return true;
            });
            return storage.approximateCount() + deltaSize[0];
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
        public boolean stream(byte[] prefix,
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
                            true, got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
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
                (txId, fp, rowType, sPrefix, sKey, sHasValue, sValue, sValueTimestamp, sValueTombstoned, sValueVersion, row)
                    -> keyValueStream.stream(sPrefix, sKey, sValue, sValueTimestamp, sValueTombstoned, sValueVersion));

            if (!complete) {
                return false;
            } else if (needsKey[0]) {
                return keyValueStream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
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
