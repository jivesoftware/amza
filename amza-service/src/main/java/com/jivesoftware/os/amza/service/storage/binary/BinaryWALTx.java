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
import java.util.concurrent.Callable;
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
    private final File key;
    private final String name;
    private final File compactingKey;
    private final File backupKey;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final int updatesBetweenLeaps;
    private final int maxLeaps;

    private final RowIOProvider ioProvider;
    private RowIO io;

    public BinaryWALTx(File baseKey,
        String name,
        RowIOProvider ioProvider,
        PrimaryRowMarshaller rowMarshaller,
        int updatesBetweenLeaps,
        int maxLeaps) throws Exception {
        this.key = ioProvider.versionedKey(baseKey, AmzaVersionConstants.LATEST_VERSION);
        this.name = name + SUFFIX;
        this.compactingKey = ioProvider.buildKey(key, "compacting");
        this.backupKey = ioProvider.buildKey(key, "backup");
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

    @Override
    public <R> R tx(Tx<R> tx) throws Exception {
        compactionLock.acquire();
        try {
            return tx.tx(io);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public <R> R readFromTransactionId(long sinceTransactionId, WALReadWithOffset<R> readWithOffset) throws Exception {
        compactionLock.acquire();
        try {
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
            return io.sizeInBytes();
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public void flush(boolean fsync) throws Exception {
        compactionLock.acquire();
        try {
            io.flush(fsync);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public <R> R open(Tx<R> tx) throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {
            initIO();
            return tx.tx(io);
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    @Override
    public <I extends CompactableWALIndex> I openIndex(WALIndexProvider<I> walIndexProvider, VersionedPartitionName versionedPartitionName) throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {
            initIO();

            I walIndex = walIndexProvider.createIndex(versionedPartitionName);
            if (walIndex.isEmpty()) {
                rebuildIndex(versionedPartitionName, walIndex, true);
            }

            return walIndex;
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    private <I extends CompactableWALIndex> void rebuildIndex(VersionedPartitionName versionedPartitionName, I walIndex, boolean fsync) throws Exception {
        LOG.info("Rebuilding {} for {}", walIndex.getClass().getSimpleName(), versionedPartitionName);

        MutableLong rebuilt = new MutableLong();
        CompactionWALIndex compactionWALIndex = walIndex.startCompaction(false);
        compactionWALIndex.merge(
            stream -> primaryRowMarshaller.fromRows(
                txFpRowStream -> {
                    // scan with allowRepairs=true to truncate at point of corruption
                    return io.scan(0, true, (rowPointer, rowTxId, rowType, row) -> {
                        if (rowType.isPrimary()) {
                            return txFpRowStream.stream(rowTxId, rowPointer, rowType, row);
                        }
                        return true;
                    });
                },
                (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row) -> {
                    rebuilt.increment();
                    return stream.stream(txId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp);
                }));
        compactionWALIndex.commit(fsync, null);

        LOG.info("Rebuilt ({}) {} for {}.", rebuilt.longValue(), walIndex.getClass().getSimpleName(), versionedPartitionName);
        walIndex.commit(fsync);
    }

    private void initIO() throws Exception {
        if (io == null) {
            io = ioProvider.open(key, name, false, updatesBetweenLeaps, maxLeaps);
            if (io == null) {
                if (ioProvider.exists(backupKey, name)) {
                    ioProvider.moveTo(backupKey, name, key, name);
                    io = ioProvider.open(key, name, false, updatesBetweenLeaps, maxLeaps);
                    if (io == null) {
                        throw new IllegalStateException("Failed to recover backup WAL " + name);
                    }
                } else {
                    io = ioProvider.open(key, name, true, updatesBetweenLeaps, maxLeaps);
                    if (io == null) {
                        throw new IllegalStateException("Failed to initialize WAL " + name);
                    }
                }
            }
        }
    }

    @Override
    public void delete() throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {
            io.close();
            ioProvider.delete(key, name);
            ioProvider.delete(backupKey, name);
            ioProvider.delete(compactingKey, name);
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    @Override
    public void hackTruncation(int numBytes) {
        io.hackTruncation(numBytes);
    }

    @Override
    public <I extends CompactableWALIndex> Compacted<I> compact(RowType compactToRowType,
        long tombstoneTimestampId,
        long tombstoneVersion,
        long ttlTimestampId,
        long ttlVersion,
        I compactableWALIndex) throws Exception {

        long start = System.currentTimeMillis();

        Preconditions.checkNotNull(compactableWALIndex, "If you don't have one use NoOpWALIndex.");
        CompactionWALIndex compactionRowIndex = compactableWALIndex.startCompaction(true);

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
        MutableLong flushTxId = new MutableLong(-1);

        byte[] carryOverEndOfMerge = null;

        long prevEndOfLastRow = 0;
        long endOfLastRow = io.getEndOfLastRow();

        while (prevEndOfLastRow < endOfLastRow) {
            try {
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
                    flushTxId,
                    tombstoneTimestampId,
                    tombstoneVersion,
                    ttlTimestampId,
                    ttlVersion,
                    null);
            } catch (Exception x) {
                LOG.error("Failure while compacting key:{} name:{} from:{} to:{}", new Object[] { key, name, prevEndOfLastRow, endOfLastRow }, x);
                compactionRowIndex.abort();
                throw x;
            }

            prevEndOfLastRow = endOfLastRow;
            endOfLastRow = io.getEndOfLastRow();
        }

        long finalEndOfLastRow = endOfLastRow;
        byte[] finalCarryOverEndOfMerge = carryOverEndOfMerge;
        return (endOfMerge) -> {
            compactionLock.acquire(NUM_PERMITS);
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
                    flushTxId,
                    tombstoneTimestampId,
                    tombstoneVersion,
                    ttlTimestampId,
                    ttlVersion,
                    endOfMerge);
                compactionIO.flush(true);

                long sizeAfterCompaction = compactionIO.sizeInBytes();
                long fpOfLastLeap = compactionIO.getFpOfLastLeap();
                long updatesSinceLeap = compactionIO.getUpdatesSinceLeap();
                compactionIO.close();
                ioProvider.delete(backupKey, name);
                if (!ioProvider.ensureKey(backupKey)) {
                    throw new IOException("Failed trying to ensure " + backupKey);
                }

                long sizeBeforeCompaction = io.sizeInBytes();

                Callable<Void> commit = () -> {
                    io.close();
                    ioProvider.moveTo(key, name, backupKey, name);
                    if (!ioProvider.ensureKey(key)) {
                        throw new IOException("Failed trying to ensure " + key);
                    }
                    ioProvider.moveTo(compactionIO.getKey(), compactionIO.getName(), key, name);
                    // Reopen the world
                    io = ioProvider.open(key, name, false, updatesBetweenLeaps, maxLeaps);
                    if (io == null) {
                        throw new IOException("Failed to reopen " + key);
                    }
                    io.flush(true);
                    io.initLeaps(fpOfLastLeap, updatesSinceLeap);

                    ioProvider.delete(backupKey, name);
                    LOG.info("Compacted partition {}/{} was:{} bytes isNow:{} bytes.", key, name, sizeBeforeCompaction, sizeAfterCompaction);
                    return null;
                };

                compactionRowIndex.commit(true, commit);

                return new CommittedCompacted<>(compactableWALIndex,
                    sizeBeforeCompaction,
                    sizeAfterCompaction,
                    keyCount.longValue(),
                    clobberCount.longValue(),
                    tombstoneCount.longValue(),
                    ttlCount.longValue(),
                    (System.currentTimeMillis() - start));
            } catch (Exception x) {
                LOG.error("Failure while compacting key:{} name:{} from:{} to end of WAL", new Object[] { key, name, finalEndOfLastRow }, x);
                compactionRowIndex.abort();
                throw x;
            } finally {
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
        MutableLong highestTxId,
        long tombstoneTimestampId,
        long tombstoneVersion,
        long ttlTimestampId,
        long ttlVersion,
        EndOfMerge endOfMerge) throws Exception {

        Preconditions.checkNotNull(compactableWALIndex, "If you don't have one use NoOpWALIndex.");

        List<CompactionFlushable> flushables = new ArrayList<>();
        MutableInt estimatedSizeInBytes = new MutableInt(0);
        MutableLong flushTxId = new MutableLong(-1);
        byte[][] keepCarryingOver = { carryOverEndOfMerge };
        primaryRowMarshaller.fromRows(
            txFpRowStream -> io.scan(startAtRow, false,
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
            (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row) -> {
                compactableWALIndex.getPointer(prefix, key, (_prefix, _key, pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp) -> {
                    if (pointerFp == -1 || CompareTimestampVersions.compare(valueTimestamp, valueVersion, pointerTimestamp, pointerVersion) >= 0) {
                        if (valueTombstoned && (valueTimestamp < tombstoneTimestampId || valueVersion < tombstoneVersion)) {
                            tombstoneCount.increment();
                        } else if (valueTimestamp < ttlTimestampId || valueVersion < ttlVersion) {
                            ttlCount.increment();
                        } else {
                            estimatedSizeInBytes.add(row.length);
                            oldestTimestamp.setValue(Math.min(valueTimestamp, oldestTimestamp.longValue()));
                            oldestVersion.setValue(Math.min(valueVersion, oldestVersion.longValue()));
                            if (valueTombstoned) {
                                oldestTombstonedTimestamp.setValue(Math.min(valueTimestamp, oldestTombstonedTimestamp.longValue()));
                                oldestTombstonedVersion.setValue(Math.min(valueVersion, oldestTombstonedVersion.longValue()));
                            }
                            flushables.add(new CompactionFlushable(prefix, key, valueTimestamp, valueTombstoned, valueVersion, row));
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
                (rowTxId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> true,
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
                if (!stream.stream(flushable.prefix, flushable.key,
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
                    (rowTxId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> stream.stream(flushTxId, prefix, key,
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
                (rowTxId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> true,
                true,
                false);
        }
    }

    public static class CompactionFlushable {

        public byte[] prefix;
        public byte[] key;
        public long valueTimestamp;
        public boolean valueTombstoned;
        public long valueVersion;
        public byte[] row;

        public CompactionFlushable(byte[] prefix, byte[] key, long valueTimestamp, boolean valueTombstoned, long valueVersion, byte[] row) {
            this.prefix = prefix;
            this.key = key;
            this.valueTimestamp = valueTimestamp;
            this.valueTombstoned = valueTombstoned;
            this.valueVersion = valueVersion;
            this.row = row;
        }
    }
}
