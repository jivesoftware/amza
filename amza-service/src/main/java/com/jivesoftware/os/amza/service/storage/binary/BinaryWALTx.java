package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.AmzaVersionConstants;
import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.CompactableWALIndex;
import com.jivesoftware.os.amza.api.scan.CompactionWALIndex;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.RowIO;
import com.jivesoftware.os.amza.api.wal.WALCompactionStats;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.api.wal.WALTx;
import com.jivesoftware.os.amza.api.wal.WALWriter.IndexableKeys;
import com.jivesoftware.os.amza.api.wal.WALWriter.RawRows;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class BinaryWALTx implements WALTx {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final String SUFFIX = ".kvt";
    private static final int NUM_PERMITS = 1024;

    private final Semaphore compactionLock = new Semaphore(NUM_PERMITS, true);
    private final String name;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final int updatesBetweenLeaps;
    private final int maxLeaps;

    private final RowIOProvider ioProvider;
    private volatile RowIO rowIO;

    public BinaryWALTx(
        String name,
        RowIOProvider ioProvider,
        PrimaryRowMarshaller rowMarshaller,
        int updatesBetweenLeaps,
        int maxLeaps) throws Exception {

        this.name = name + SUFFIX;
        this.primaryRowMarshaller = rowMarshaller;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
        this.maxLeaps = maxLeaps;
        this.ioProvider = ioProvider;
    }

    public static Set<String> listExisting(File baseKey, RowIOProvider ioProvider) throws Exception {
        File key = ioProvider.versionedKey(baseKey, AmzaVersionConstants.LATEST_VERSION);
        List<String> names = ioProvider.listExisting(key);
        Set<String> matched = Sets.newHashSet();
        for (String name : names) {
            if (name.endsWith(SUFFIX)) {
                matched.add(name.substring(0, name.indexOf(SUFFIX)));
            }
        }
        return matched;
    }

    public static long sizeInBytes(File baseKey, String name, RowIOProvider ioProvider) throws Exception {
        File key = ioProvider.versionedKey(baseKey, AmzaVersionConstants.LATEST_VERSION);
        return ioProvider.sizeInBytes(key, name);
    }

    @Override
    public <R> R tx(Tx<R> tx) throws Exception {
        compactionLock.acquire();
        try {
            return tx.tx(rowIO);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public <R> R readFromTransactionId(long sinceTransactionId, WALReadWithOffset<R> readWithOffset) throws Exception {
        compactionLock.acquire();
        try {
            RowIO io = rowIO;
            long offset = io.getInclusiveStartOfRow(sinceTransactionId);
            return readWithOffset.read(offset, io);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public long length() throws Exception {
        compactionLock.acquire();
        try {
            return rowIO.sizeInBytes();
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public void flush(boolean fsync) throws Exception {
        compactionLock.acquire();
        try {
            rowIO.flush(fsync);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public <R> R open(File baseKey, Tx<R> tx) throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {
            initIO(baseKey);
            return tx.tx(rowIO);
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    @Override
    public <I extends CompactableWALIndex> I openIndex(File baseKey, WALIndexProvider<I> walIndexProvider,
        VersionedPartitionName versionedPartitionName,
        int maxValueSizeInIndex,
        int stripe) throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {
            initIO(baseKey);

            I walIndex = walIndexProvider.createIndex(versionedPartitionName, maxValueSizeInIndex, stripe);
            if (!walIndex.exists()) {
                rebuildIndex(versionedPartitionName, walIndex, true);
            }

            return walIndex;
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    private <I extends CompactableWALIndex> void rebuildIndex(VersionedPartitionName versionedPartitionName,
        I compactableWALIndex,
        boolean fsync) throws Exception {

        boolean[] isEmpty = { true };
        rowIO.scan(0, true, (rowPointer, rowTxId, rowType, row) -> {
            if (rowType.isPrimary()) {
                isEmpty[0] = false;
                return false;
            }
            return true;
        });

        if (isEmpty[0]) {
            LOG.info("Skipping {} rebuild for {} because the WAL is empty, length={}",
                compactableWALIndex.getClass().getSimpleName(), versionedPartitionName, rowIO.sizeInBytes());
            return;
        }

        LOG.info("Rebuilding {} for {}", compactableWALIndex.getClass().getSimpleName(), versionedPartitionName);

        MutableLong rebuilt = new MutableLong();
        CompactionWALIndex compactionWALIndex = compactableWALIndex.startCompaction(false, compactableWALIndex.getStripe());
        compactionWALIndex.merge(
            stream -> primaryRowMarshaller.fromRows(
                txFpRowStream -> {
                    // scan with allowRepairs=true to truncate at point of corruption
                    return rowIO.scan(0, true, (rowPointer, rowTxId, rowType, row) -> {
                        if (rowType.isPrimary()) {
                            return txFpRowStream.stream(rowTxId, rowPointer, rowType, row);
                        }
                        return true;
                    });
                },
                (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, row) -> {
                    rebuilt.increment();
                    return stream.stream(txId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, fp);
                }));
        compactionWALIndex.commit(fsync, null);

        LOG.info("Rebuilt ({}) {} for {}.", rebuilt.longValue(), compactableWALIndex.getClass().getSimpleName(), versionedPartitionName);
        compactableWALIndex.commit(fsync);
    }

    private void initIO(File baseKey) throws Exception {
        if (rowIO == null) {
            File key = ioProvider.versionedKey(baseKey, AmzaVersionConstants.LATEST_VERSION);
            rowIO = ioProvider.open(key, name, false, updatesBetweenLeaps, maxLeaps);
            if (rowIO == null) {
                File backupKey = ioProvider.buildKey(key, "backup");
                if (ioProvider.exists(backupKey, name)) {
                    ioProvider.moveTo(backupKey, name, key, name);
                    rowIO = ioProvider.open(key, name, false, updatesBetweenLeaps, maxLeaps);
                    if (rowIO == null) {
                        throw new IllegalStateException("Failed to recover backup WAL " + name);
                    }
                } else {
                    rowIO = ioProvider.open(key, name, true, updatesBetweenLeaps, maxLeaps);
                    if (rowIO == null) {
                        throw new IllegalStateException("Failed to initialize WAL " + name);
                    }
                }
            }
        }
    }

    @Override
    public void delete(File baseKey) throws Exception {
        File key = ioProvider.versionedKey(baseKey, AmzaVersionConstants.LATEST_VERSION);
        compactionLock.acquire(NUM_PERMITS);
        try {
            rowIO.close();
            File compactingKey = ioProvider.buildKey(key, "compacting");
            File backupKey = ioProvider.buildKey(key, "backup");
            ioProvider.delete(key, name);
            ioProvider.delete(backupKey, name);
            ioProvider.delete(compactingKey, name);
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    @Override
    public void hackTruncation(int numBytes) {
        rowIO.hackTruncation(numBytes);
    }

    @Override
    public <I extends CompactableWALIndex> Compacted<I> compact(WALCompactionStats compactionStats,
        File fromBaseKey,
        File toBaseKey,
        RowType compactToRowType,
        long tombstoneTimestampId,
        long tombstoneVersion,
        long ttlTimestampId,
        long ttlVersion,
        long disposalVersion,
        I compactableWALIndex,
        int stripe) throws Exception {

        File fromKey = ioProvider.versionedKey(fromBaseKey, AmzaVersionConstants.LATEST_VERSION);
        File toKey = ioProvider.versionedKey(toBaseKey, AmzaVersionConstants.LATEST_VERSION);

        long start = System.currentTimeMillis();

        Preconditions.checkNotNull(compactableWALIndex, "If you don't have one use NoOpWALIndex.");
        CompactionWALIndex compactionRowIndex = compactableWALIndex.startCompaction(true, stripe);

        File compactingKey = ioProvider.buildKey(toKey, "compacting");
        File backupKey = ioProvider.buildKey(fromKey, "backup");

        ioProvider.delete(compactingKey, name);
        if (!ioProvider.ensureKey(compactingKey)) {
            throw new IOException("Failed remove " + compactingKey);
        }
        RowIO compactionIO = ioProvider.open(compactingKey, name, true, updatesBetweenLeaps, maxLeaps);
        compactionIO.initLeaps(-1, 0);

        MutableLong oldestTimestamp = new MutableLong(Long.MAX_VALUE);
        MutableLong oldestVersion = new MutableLong(Long.MAX_VALUE);
        MutableLong oldestTombstonedTimestamp = new MutableLong(Long.MAX_VALUE);
        MutableLong oldestTombstonedVersion = new MutableLong(Long.MAX_VALUE);
        MutableLong keyCount = new MutableLong();
        MutableLong clobberCount = new MutableLong();
        MutableLong tombstoneCount = new MutableLong();
        MutableLong ttlCount = new MutableLong();
        MutableLong disposalCount = new MutableLong();
        MutableLong flushTxId = new MutableLong(-1);

        byte[] carryOverEndOfMerge = null;

        long endOfLastRow;
        compactionLock.acquire();
        try {
            long prevEndOfLastRow = 0;
            endOfLastRow = rowIO.getEndOfLastRow();

            int compactionPass = 1;
            while (prevEndOfLastRow < endOfLastRow) {
                try {
                    compactionStats.add("compact", 1);
                    compactionStats.start("compact-" + compactionPass);
                    carryOverEndOfMerge = compact(compactToRowType,
                        prevEndOfLastRow,
                        endOfLastRow,
                        carryOverEndOfMerge,
                        compactableWALIndex,
                        compactionRowIndex,
                        compactionIO,
                        oldestTimestamp,
                        oldestVersion,
                        oldestTombstonedTimestamp,
                        oldestTombstonedVersion,
                        keyCount,
                        clobberCount,
                        tombstoneCount,
                        ttlCount,
                        disposalCount,
                        flushTxId,
                        tombstoneTimestampId,
                        tombstoneVersion,
                        ttlTimestampId,
                        ttlVersion,
                        disposalVersion,
                        null);
                } catch (Exception x) {
                    LOG.error("Failure while compacting fromKey:{} -> toKey:{} name:{} from:{} to:{}",
                        new Object[]{fromKey, toKey, name, prevEndOfLastRow, endOfLastRow}, x);
                    compactionRowIndex.abort();
                    throw x;
                } finally {
                    compactionStats.stop("compact-" + compactionPass);
                }

                prevEndOfLastRow = endOfLastRow;
                endOfLastRow = rowIO.getEndOfLastRow();
                compactionPass++;
            }
        } finally {
            compactionLock.release();
        }

        long finalEndOfLastRow = endOfLastRow;
        byte[] finalCarryOverEndOfMerge = carryOverEndOfMerge;

        int[] completionPass = {1};
        return (endOfMerge, completedCompactCommit) -> {
            compactionLock.acquire(NUM_PERMITS);
            try {
                compactionStats.add("completion-all", 1);
                compactionStats.start("completion-all" + completionPass[0]);
                compactionStats.start("completion-compact-" + completionPass[0]);
                try {
                    compact(compactToRowType,
                        finalEndOfLastRow,
                        Long.MAX_VALUE,
                        finalCarryOverEndOfMerge,
                        compactableWALIndex,
                        compactionRowIndex,
                        compactionIO,
                        oldestTimestamp,
                        oldestVersion,
                        oldestTombstonedTimestamp,
                        oldestTombstonedVersion,
                        keyCount,
                        clobberCount,
                        tombstoneCount,
                        ttlCount,
                        disposalCount,
                        flushTxId,
                        tombstoneTimestampId,
                        tombstoneVersion,
                        ttlTimestampId,
                        ttlVersion,
                        disposalVersion,
                        endOfMerge);
                } finally {
                    compactionStats.stop("completion-compact-" + completionPass[0]);
                }
                compactionStats.start("completion-flush-" + completionPass[0]);
                long sizeAfterCompaction;
                long fpOfLastLeap;
                long updatesSinceLeap;
                try {
                    compactionIO.flush(true);
                    sizeAfterCompaction = compactionIO.sizeInBytes();
                    fpOfLastLeap = compactionIO.getFpOfLastLeap();
                    updatesSinceLeap = compactionIO.getUpdatesSinceLeap();
                    compactionIO.close();
                    ioProvider.delete(backupKey, name);
                    if (!ioProvider.ensureKey(backupKey)) {
                        throw new IOException("Failed trying to ensure " + backupKey);
                    }
                } finally {
                    compactionStats.stop("completion-flush-" + completionPass[0]);
                }

                long sizeBeforeCompaction = rowIO.sizeInBytes();
                compactionStats.start("completion-commit-" + completionPass[0]);
                try {
                    compactionRowIndex.commit(true, () -> {
                        compactionStats.start("completion-commit-swap-" + completionPass[0]);
                        try {
                            rowIO.close();
                            ioProvider.moveTo(fromKey, name, backupKey, name);
                            if (!ioProvider.ensureKey(toKey)) {
                                throw new IOException("Failed trying to ensure " + toKey);
                            }
                            ioProvider.delete(toKey, name);
                            ioProvider.moveTo(compactionIO.getKey(), compactionIO.getName(), toKey, name);
                        } finally {
                            compactionStats.stop("completion-commit-swap-" + completionPass[0]);
                        }
                        // Reopen the world
                        compactionStats.start("completion-commit-reopen-" + completionPass[0]);
                        try {
                            RowIO io = ioProvider.open(toKey, name, false, updatesBetweenLeaps, maxLeaps);
                            rowIO = io;
                            if (rowIO == null) {
                                throw new IOException("Failed to reopen " + toKey);
                            }
                            rowIO.flush(true);
                            rowIO.initLeaps(fpOfLastLeap, updatesSinceLeap);
                        } finally {
                            compactionStats.stop("completion-commit-reopen-" + completionPass[0]);
                        }
                        compactionStats.start("completion-commit-finalize-" + completionPass[0]);
                        try {
                            completedCompactCommit.call();

                            ioProvider.delete(backupKey, name);
                            LOG.info("Compacted partition fromKey:{} -> toKey:{} named:{} was:{} bytes isNow:{} bytes.",
                                fromKey, toKey, name, sizeBeforeCompaction, sizeAfterCompaction);
                        } finally {
                            compactionStats.stop("completion-commit-finalize-" + completionPass[0]);
                        }
                        return null;
                    });
                } finally {
                    compactionStats.stop("completion-commit-" + completionPass[0]);
                }
                return new CommittedCompacted<>(compactableWALIndex,
                    sizeBeforeCompaction,
                    sizeAfterCompaction,
                    keyCount.longValue(),
                    clobberCount.longValue(),
                    tombstoneCount.longValue(),
                    ttlCount.longValue(),
                    disposalCount.longValue(),
                    oldestTimestamp.longValue() == Long.MAX_VALUE ? -1 : oldestTimestamp.longValue(),
                    oldestVersion.longValue() == Long.MAX_VALUE ? -1 : oldestVersion.longValue(),
                    oldestTombstonedTimestamp.longValue() == Long.MAX_VALUE ? -1 : oldestTombstonedTimestamp.longValue(),
                    oldestTombstonedVersion.longValue() == Long.MAX_VALUE ? -1 : oldestTombstonedVersion.longValue(),
                    (System.currentTimeMillis() - start));
            } catch (Exception x) {
                LOG.error("Failure while compacting fromKey:{} -> toKey:{} name:{} from:{} to end of WAL",
                    new Object[]{fromKey, toKey, name, finalEndOfLastRow}, x);
                compactionRowIndex.abort();
                throw x;
            } finally {
                compactionStats.stop("completion-all" + completionPass[0]);
                completionPass[0]++;
                compactionLock.release(NUM_PERMITS);
            }
        };

    }

    private byte[] compact(RowType compactToRowType,
        long startAtRow,
        long endOfLastRow,
        byte[] carryOverEndOfMerge,
        CompactableWALIndex compactableWALIndex,
        CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        MutableLong oldestTimestamp,
        MutableLong oldestVersion,
        MutableLong oldestTombstonedTimestamp,
        MutableLong oldestTombstonedVersion,
        MutableLong keyCount,
        MutableLong clobberCount,
        MutableLong tombstoneCount,
        MutableLong ttlCount,
        MutableLong disposalCount,
        MutableLong highestTxId,
        long tombstoneTimestampId,
        long tombstoneVersion,
        long ttlTimestampId,
        long ttlVersion,
        long disposalVersion,
        EndOfMerge endOfMerge) throws Exception {

        Preconditions.checkNotNull(compactableWALIndex, "If you don't have one use NoOpWALIndex.");

        List<CompactionFlushable> flushables = new ArrayList<>();
        MutableInt estimatedSizeInBytes = new MutableInt(0);
        MutableLong flushTxId = new MutableLong(-1);
        byte[][] keepCarryingOver = {carryOverEndOfMerge};
        primaryRowMarshaller.fromRows(
            txFpRowStream -> rowIO.scan(startAtRow, false,
                (rowFP, rowTxId, rowType, row) -> {
                    if (rowFP >= endOfLastRow) {
                        return false;
                    }
                    if (rowType.isDiscardedDuringCompactions()) {
                        return true;
                    }

                    if (flushTxId.longValue() != rowTxId) {
                        if (flushTxId.longValue() != -1 && !flushables.isEmpty()) {
                            flushBatch(compactToRowType,
                                compactionWALIndex,
                                compactionIO,
                                flushables.size(),
                                estimatedSizeInBytes.intValue(),
                                flushables,
                                flushTxId.longValue());
                            flushables.clear();
                        }
                        estimatedSizeInBytes.setValue(0);
                        flushTxId.setValue(rowTxId);
                    }

                    if (rowType.isPrimary()) {
                        highestTxId.setValue(Math.max(highestTxId.longValue(), rowTxId));
                        byte[] convertedRow = primaryRowMarshaller.convert(rowType, row, compactToRowType);
                        if (!txFpRowStream.stream(rowTxId, rowFP, compactToRowType, convertedRow)) {
                            return false;
                        }
                    } else if (rowType == RowType.highwater) {
                        compactionIO.writeHighwater(row);
                    } else if (rowType == RowType.end_of_merge) {
                        keepCarryingOver[0] = row;
                    } else {
                        // system is ignored
                    }
                    return true;
                }),
            (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, row) -> {
                compactableWALIndex.getPointer(prefix, key,
                    (_prefix, _key, pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp, hasValue1, value1) -> {
                        if ((pointerFp == -1 && !hasValue1)
                            || CompareTimestampVersions.compare(valueTimestamp, valueVersion, pointerTimestamp, pointerVersion) >= 0) {
                            if (valueTombstoned && (valueTimestamp < tombstoneTimestampId || valueVersion < tombstoneVersion)) {
                                tombstoneCount.increment();
                            } else if (valueTimestamp < ttlTimestampId || valueVersion < ttlVersion) {
                                ttlCount.increment();
                            } else if (valueVersion < disposalVersion) {
                                disposalCount.increment();
                            } else {
                                estimatedSizeInBytes.add(row.length);
                                oldestTimestamp.setValue(Math.min(valueTimestamp, oldestTimestamp.longValue()));
                                oldestVersion.setValue(Math.min(valueVersion, oldestVersion.longValue()));
                                if (valueTombstoned) {
                                    oldestTombstonedTimestamp.setValue(Math.min(valueTimestamp, oldestTombstonedTimestamp.longValue()));
                                    oldestTombstonedVersion.setValue(Math.min(valueVersion, oldestTombstonedVersion.longValue()));
                                }
                                flushables.add(new CompactionFlushable(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row));
                                keyCount.increment();
                            }
                        } else {
                            clobberCount.increment();
                        }
                        return true;
                    });
                return true;
            });
        if (endOfMerge != null && keepCarryingOver[0] == null) {
            throw new IllegalStateException("Failed to encounter an end of merge hint while compacting.");
        }

        if (flushTxId.longValue() != -1 && !flushables.isEmpty()) {
            flushBatch(compactToRowType,
                compactionWALIndex,
                compactionIO,
                flushables.size(),
                estimatedSizeInBytes.intValue(),
                flushables,
                flushTxId.longValue());
        }

        if (endOfMerge != null && carryOverEndOfMerge != null) {
            byte[] finallyAnEndOfMerge = endOfMerge.endOfMerge(carryOverEndOfMerge,
                highestTxId.longValue(),
                oldestTimestamp.longValue() == Long.MAX_VALUE ? -1 : oldestTimestamp.longValue(),
                oldestVersion.longValue() == Long.MAX_VALUE ? -1 : oldestVersion.longValue(),
                oldestTombstonedTimestamp.longValue() == Long.MAX_VALUE ? -1 : oldestTombstonedTimestamp.longValue(),
                oldestTombstonedVersion.longValue() == Long.MAX_VALUE ? -1 : oldestTombstonedVersion.longValue(),
                keyCount.longValue(),
                compactionIO.getFpOfLastLeap(),
                compactionIO.getUpdatesSinceLeap());

            compactionIO.write(highestTxId.longValue(),
                RowType.end_of_merge,
                1,
                finallyAnEndOfMerge.length,
                stream -> stream.stream(finallyAnEndOfMerge),
                stream -> true,
                (rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, fp) -> true,
                true,
                false);
        }

        estimatedSizeInBytes.setValue(0);
        flushables.clear();
        return keepCarryingOver[0];
    }

    private void flushBatch(RowType rowType,
        CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        int estimatedNumberOfRows,
        int estimatedSizeInBytes,
        List<CompactionFlushable> flushables,
        long flushTxId) throws Exception {

        RawRows rows = stream -> {
            for (CompactionFlushable flushable : flushables) {
                if (!stream.stream(flushable.row)) {
                    return false;
                }
            }
            return true;
        };

        IndexableKeys indexableKeys = stream -> {
            for (CompactionFlushable flushable : flushables) {
                if (!stream.stream(flushable.prefix, flushable.key, flushable.value,
                    flushable.valueTimestamp, flushable.valueTombstoned, flushable.valueVersion)) {
                    return false;
                }
            }
            return true;
        };

        if (compactionWALIndex != null) {
            compactionWALIndex.merge((stream) -> {
                compactionIO.write(flushTxId,
                    rowType,
                    estimatedNumberOfRows,
                    estimatedSizeInBytes,
                    rows,
                    indexableKeys,
                    (rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, fp) -> stream.stream(flushTxId, prefix, key, value,
                        valueTimestamp, valueTombstoned, valueVersion, fp),
                    true,
                    false);
                return true;
            });
        } else {
            compactionIO.write(flushTxId,
                rowType,
                estimatedNumberOfRows,
                estimatedSizeInBytes,
                rows,
                indexableKeys,
                (rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, fp) -> true,
                true,
                false);
        }
    }

    public static class CompactionFlushable {

        public byte[] prefix;
        public byte[] key;
        public byte[] value;
        public long valueTimestamp;
        public boolean valueTombstoned;
        public long valueVersion;
        public byte[] row;

        public CompactionFlushable(byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion, byte[] row) {
            this.prefix = prefix;
            this.key = key;
            this.value = value;
            this.valueTimestamp = valueTimestamp;
            this.valueTombstoned = valueTombstoned;
            this.valueVersion = valueVersion;
            this.row = row;
        }
    }
}
