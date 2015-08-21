package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.AmzaVersionConstants;
import com.jivesoftware.os.amza.shared.scan.CompactableWALIndex;
import com.jivesoftware.os.amza.shared.scan.CompactableWALIndex.CompactionWALIndex;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.shared.stream.TxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import com.jivesoftware.os.amza.shared.wal.WALWriter.IndexableKeys;
import com.jivesoftware.os.amza.shared.wal.WALWriter.RawRows;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class BinaryWALTx<I extends CompactableWALIndex> implements WALTx<I> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final String SUFFIX = ".kvt";
    private static final int NUM_PERMITS = 1024;

    private final Semaphore compactionLock = new Semaphore(NUM_PERMITS, true);
    private final File dir;
    private final String name;
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final WALIndexProvider<I> walIndexProvider;

    private final RowIOProvider ioProvider;
    private RowIO<File> io;

    public BinaryWALTx(File baseDir,
        String prefix,
        RowIOProvider ioProvider,
        PrimaryRowMarshaller<byte[]> rowMarshaller,
        WALIndexProvider<I> walIndexProvider) throws Exception {
        this.dir = new File(baseDir, AmzaVersionConstants.LATEST_VERSION);
        this.name = prefix + SUFFIX;
        this.ioProvider = ioProvider;
        this.primaryRowMarshaller = rowMarshaller;
        this.walIndexProvider = walIndexProvider;
        this.io = ioProvider.create(dir, name);
    }

    public static Set<String> listExisting(File baseDir, RowIOProvider ioProvider) {
        File dir = new File(baseDir, AmzaVersionConstants.LATEST_VERSION);
        List<File> files = ioProvider.listExisting(dir);
        Set<String> names = Sets.newHashSet();
        for (File file : files) {
            String name = file.getName();
            if (name.endsWith(SUFFIX)) {
                names.add(name.substring(0, name.indexOf(SUFFIX)));
            }
        }
        return names;
    }

    @Override
    public <R> R write(WALWrite<R> write) throws Exception {
        compactionLock.acquire();
        try {
            return write.write(io);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public <R> R read(WALRead<R> read) throws Exception {
        compactionLock.acquire();
        try {
            return read.read(io);
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
        return io.sizeInBytes();
    }

    @Override
    public void flush(boolean fsync) throws Exception {
        io.flush(fsync);
    }

    @Override
    public void validateAndRepair() throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {
            if (!io.validate()) {
                LOG.info("Recovering for WAL {}", name);
                final MutableLong count = new MutableLong(0);
                io.scan(0, true, (final long rowPointer, long rowTxId, RowType rowType, byte[] row) -> {
                    count.increment();
                    return true;
                });
                LOG.info("Recovered for WAL {}: {} rows", name, count.longValue());
            }
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    @Override
    public I load(VersionedPartitionName versionedPartitionName) throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {

            if (!io.validate()) {
                LOG.warn("Encountered a corrupt WAL. Removing wal index for {} ...", versionedPartitionName);
                walIndexProvider.deleteIndex(versionedPartitionName);
                LOG.warn("Removed wal index for {}.", versionedPartitionName);
            }

            final I walIndex = walIndexProvider.createIndex(versionedPartitionName);
            if (walIndex.isEmpty()) {
                LOG.info("Rebuilding {} for {}", walIndex.getClass().getSimpleName(), versionedPartitionName);

                MutableLong rebuilt = new MutableLong();
                walIndex.merge(
                    stream -> primaryRowMarshaller.fromRows(txFpRowStream -> {
                        return io.scan(0, true,
                            (rowPointer, rowTxId, rowType, row) -> {
                                if (rowType == RowType.primary) {
                                    return txFpRowStream.stream(rowTxId, rowPointer, row);
                                }
                                return true;
                            });
                    }, (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, row) -> {
                        rebuilt.increment();
                        return stream.stream(txId, prefix, key, valueTimestamp, valueTombstoned, fp);
                    }),
                    (mode, txId, prefix, key, timestamp, tombstoned, fp) -> true);

                LOG.info("Rebuilt ({}) {} for {}.", rebuilt.longValue(), walIndex.getClass().getSimpleName(), versionedPartitionName);
                walIndex.commit();
            } else {
                LOG.info("Checking {} for {}.", walIndex.getClass().getSimpleName(), versionedPartitionName);

                final MutableLong repair = new MutableLong();
                walIndex.merge(
                    stream -> primaryRowMarshaller.fromRows(txFpRowStream -> {
                        long[] commitedUpToTxId = { Long.MIN_VALUE };
                        return io.reverseScan((rowFP, rowTxId, rowType, row) -> {
                            if (rowType == RowType.primary) {
                                if (!txFpRowStream.stream(rowTxId, rowFP, row)) {
                                    return false;
                                }
                            }
                            if (rowType == RowType.system && commitedUpToTxId[0] == Long.MIN_VALUE) {
                                long[] key_CommitedUpToTxId = UIO.bytesLongs(row);
                                if (key_CommitedUpToTxId[0] == RowType.COMMIT_KEY) {
                                    commitedUpToTxId[0] = key_CommitedUpToTxId[1];
                                    LOG.info("Looking for txId:{} for versionedPartitionName:{} ", commitedUpToTxId, versionedPartitionName);
                                }
                                return true;
                            } else {
                                return rowTxId >= commitedUpToTxId[0];
                            }
                        });
                    }, (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, row) -> {
                        repair.increment();
                        return stream.stream(txId, prefix, key, valueTimestamp, valueTombstoned, fp);
                    }),
                    (mode, txId, prefix, key, timestamp, tombstoned, fp) -> true);

                LOG.info("Checked ({}) {} for {}.", repair.longValue(), walIndex.getClass().getSimpleName(), versionedPartitionName);
                walIndex.commit();
            }
            io.initLeaps();
            return walIndex;
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    @Override
    public boolean delete(boolean ifEmpty) throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {
            if (!ifEmpty || io.sizeInBytes() == 0) {
                try {
                    io.close();
                } catch (Exception x) {
                    LOG.warn("Failed to close IO before deleting WAL: {}", new Object[] { dir.getAbsolutePath() }, x);
                }
                io.delete();
                return true;
            }
            return false;
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    @Override
    public Optional<Compacted<I>> compact(long removeTombstonedOlderThanTimestampId,
        long ttlTimestampId,
        I compactableWALIndex,
        boolean force) throws Exception {

        long endOfLastRow = io.getEndOfLastRow();
        long start = System.currentTimeMillis();

        CompactionWALIndex compactionRowIndex = compactableWALIndex != null ? compactableWALIndex.startCompaction() : null;

        File tempDir = Files.createTempDir();
        RowIO<File> compactionIO = ioProvider.create(tempDir, name);

        AtomicLong keyCount = new AtomicLong();
        AtomicLong clobberCount = new AtomicLong();
        AtomicLong tombstoneCount = new AtomicLong();
        AtomicLong ttlCount = new AtomicLong();

        compact(0,
            endOfLastRow,
            compactableWALIndex,
            compactionRowIndex,
            compactionIO,
            keyCount,
            clobberCount,
            tombstoneCount,
            ttlCount,
            removeTombstonedOlderThanTimestampId,
            ttlTimestampId);

        return Optional.of(() -> {
            compactionLock.acquire(NUM_PERMITS);
            try {
                long startCatchup = System.currentTimeMillis();
                AtomicLong catchupKeyCount = new AtomicLong();
                AtomicLong catchupClobberCount = new AtomicLong();
                AtomicLong catchupTombstoneCount = new AtomicLong();
                AtomicLong catchupTTLCount = new AtomicLong();
                compact(endOfLastRow, Long.MAX_VALUE, compactableWALIndex, compactionRowIndex, compactionIO,
                    catchupKeyCount, catchupClobberCount, catchupTombstoneCount, catchupTTLCount,
                    removeTombstonedOlderThanTimestampId,
                    ttlTimestampId);
                compactionIO.flush(true);
                long sizeAfterCompaction = compactionIO.sizeInBytes();
                compactionIO.close();
                File backup = new File(dir, "bkp");
                backup.delete();
                if (!backup.exists() && !backup.mkdirs()) {
                    throw new IOException("Failed trying to mkdirs for " + backup);
                }
                long sizeBeforeCompaction = io.sizeInBytes();
                io.close();
                io.move(backup);
                if (!dir.exists() && !dir.mkdirs()) {
                    throw new IOException("Failed trying to mkdirs for " + dir);
                }
                compactionIO.move(dir);
                // Reopen the world
                io = ioProvider.create(dir, name);
                LOG.info("Compacted partition " + dir.getAbsolutePath() + "/" + name
                    + " was:" + sizeBeforeCompaction + "bytes "
                    + " isNow:" + sizeAfterCompaction + "bytes.");
                if (compactionRowIndex != null) {
                    compactionRowIndex.commit();
                }
                return new CommittedCompacted<>(compactableWALIndex,
                    sizeBeforeCompaction,
                    sizeAfterCompaction,
                    keyCount.longValue(),
                    clobberCount.longValue(),
                    tombstoneCount.longValue(),
                    ttlCount.longValue(),
                    System.currentTimeMillis() - start,
                    catchupKeyCount.longValue(),
                    catchupClobberCount.longValue(),
                    catchupTombstoneCount.longValue(),
                    catchupTTLCount.longValue(),
                    System.currentTimeMillis() - startCatchup);
            } finally {
                compactionLock.release(NUM_PERMITS);
            }
        });

    }

    private void compact(long startAtRow,
        long endOfLastRow,
        CompactableWALIndex compactableWALIndex,
        CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        AtomicLong keyCount,
        AtomicLong clobberCount,
        AtomicLong tombstoneCount,
        AtomicLong ttlCount,
        long removeTombstonedOlderThanTimestampId,
        long ttlTimestampId) throws Exception {

        Preconditions.checkNotNull(compactableWALIndex, "If you don't have one use NOOpWALIndex.");

        List<CompactionFlushable> flushables = new ArrayList<>();
        MutableInt estimatedSizeInBytes = new MutableInt(0);
        MutableLong flushTxId = new MutableLong(-1);
        primaryRowMarshaller.fromRows(
            txFpRowStream -> io.scan(startAtRow, false,
                (rowFP, rowTxId, rowType, row) -> {
                    if (rowFP >= endOfLastRow) {
                        return false;
                    }
                    if (rowType.isDiscardedDuringCompactions()) {
                        return true;
                    }

                    long lastTxId = flushTxId.longValue();
                    if (lastTxId != rowTxId) {
                        flushBatch(compactionWALIndex,
                            compactionIO,
                            flushables.size(),
                            estimatedSizeInBytes.intValue(),
                            flushables,
                            lastTxId);
                        flushables.clear();
                        estimatedSizeInBytes.setValue(0);
                        flushTxId.setValue(rowTxId);
                    }

                    if (rowType == RowType.primary) {
                        if (!txFpRowStream.stream(rowTxId, rowFP, row)) {
                            return false;
                        }
                    } else if (rowType == RowType.highwater) {
                        compactionIO.writeHighwater(row);
                    } else {
                        // system is ignored
                    }
                    return true;
                }),
            (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, row) -> {
                compactableWALIndex.getPointer(prefix, key, (_prefix, _key, pointerTimestamp, pointerTombstoned, pointerFp) -> {
                    if (pointerFp == -1 || valueTimestamp >= pointerTimestamp) {
                        if (valueTombstoned && valueTimestamp < removeTombstonedOlderThanTimestampId) {
                            tombstoneCount.incrementAndGet();
                        } else {
                            if (valueTimestamp > ttlTimestampId) {
                                estimatedSizeInBytes.add(row.length);
                                flushables.add(new CompactionFlushable(prefix, key, valueTimestamp, valueTombstoned, row));
                                keyCount.incrementAndGet();
                            } else {
                                ttlCount.incrementAndGet();
                            }
                        }
                    } else {
                        clobberCount.incrementAndGet();
                    }
                    return true;
                });
                return true;
            });
        flushBatch(compactionWALIndex,
            compactionIO,
            flushables.size(),
            estimatedSizeInBytes.intValue(),
            flushables,
            flushTxId.longValue());
        estimatedSizeInBytes.setValue(0);
        flushables.clear();
    }

    private void flushBatch(CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        int estimatedNumberOfRows,
        int estimatedSizeInBytes,
        List<CompactionFlushable> flushables,
        long lastTxId) throws Exception {

        if (flushables.isEmpty()) {
            return;
        }
        flush(compactionWALIndex,
            compactionIO,
            lastTxId,
            RowType.primary,
            estimatedNumberOfRows,
            estimatedSizeInBytes,
            rowStream -> {
                for (CompactionFlushable flushable : flushables) {
                    if (!rowStream.stream(flushable.row)) {
                        return false;
                    }
                }
                return true;
            },
            indexableKeyStream -> {
                for (CompactionFlushable flushable : flushables) {
                    if (!indexableKeyStream.stream(flushable.prefix, flushable.key, flushable.valueTimestamp, flushable.valueTombstoned)) {
                        return false;
                    }
                }
                return true;
            });
    }

    private void flush(CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        long txId,
        RowType rowType,
        int estimatedNumberOfRows,
        int estimatedSizeInBytes,
        RawRows rows,
        IndexableKeys indexableKeys) throws Exception {

        if (compactionWALIndex != null) {
            compactionWALIndex.merge((TxKeyPointerStream stream) -> {
                compactionIO.write(txId, rowType, estimatedNumberOfRows, estimatedSizeInBytes, rows, indexableKeys,
                    (rowTxId, prefix, key, valueTimestamp, valueTombstoned, fp) -> stream.stream(txId, prefix, key, valueTimestamp, valueTombstoned, fp));
                return true;
            });
        } else {
            compactionIO.write(txId, rowType, estimatedNumberOfRows, estimatedSizeInBytes, rows, indexableKeys,
                (rowTxId, prefix, key, valueTimestamp, valueTombstoned, fp) -> true);
        }
    }

    public static class CompactionFlushable {

        public byte[] prefix;
        public byte[] key;
        public long valueTimestamp;
        public boolean valueTombstoned;
        public byte[] row;

        public CompactionFlushable(byte[] prefix, byte[] key, long valueTimestamp, boolean valueTombstoned, byte[] row) {
            this.prefix = prefix;
            this.key = key;
            this.valueTimestamp = valueTimestamp;
            this.valueTombstoned = valueTombstoned;
            this.row = row;
        }
    }
}
