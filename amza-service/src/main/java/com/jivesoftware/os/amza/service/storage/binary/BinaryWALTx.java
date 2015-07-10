package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.AmzaVersionConstants;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALIndex.CompactionWALIndex;
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
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class BinaryWALTx implements WALTx {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final String SUFFIX = ".kvt";
    private static final int NUM_PERMITS = 1024;

    private final Semaphore compactionLock = new Semaphore(NUM_PERMITS, true);
    private final File dir;
    private final String name;
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final WALIndexProvider walIndexProvider;
    private final int compactAfterGrowthFactor;
    private final AtomicLong lastEndOfLastRow = new AtomicLong(-1);

    private final RowIOProvider ioProvider;
    private RowIO<File> io;

    public BinaryWALTx(File baseDir,
        String prefix,
        RowIOProvider ioProvider,
        PrimaryRowMarshaller<byte[]> rowMarshaller,
        WALIndexProvider walIndexProvider,
        int compactAfterGrowthFactor) throws Exception {
        this.dir = new File(baseDir, AmzaVersionConstants.LATEST_VERSION);
        this.name = prefix + SUFFIX;
        this.ioProvider = ioProvider;
        this.primaryRowMarshaller = rowMarshaller;
        this.walIndexProvider = walIndexProvider;
        this.compactAfterGrowthFactor = compactAfterGrowthFactor;
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
    public WALIndex load(VersionedPartitionName versionedPartitionName) throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {

            if (!io.validate()) {
                LOG.warn("Encountered a corrupt WAL. Removing wal index for {} ...", versionedPartitionName);
                walIndexProvider.deleteIndex(versionedPartitionName);
                LOG.warn("Removed wal index for {}.", versionedPartitionName);
            }

            final WALIndex walIndex = walIndexProvider.createIndex(versionedPartitionName);
            if (walIndex.isEmpty()) {
                LOG.info("Rebuilding {} for {}", walIndex.getClass().getSimpleName(), versionedPartitionName);

                MutableLong rebuilt = new MutableLong();
                walIndex.merge((TxKeyPointerStream stream) -> {
                    return io.scan(0, true, (long rowPointer, long rowTxId, RowType rowType, byte[] row) -> {
                        if (rowType == RowType.primary) {
                            primaryRowMarshaller.fromRow(row, (key, value, valueTimestamp, valueTombstoned) -> {
                                stream.stream(rowTxId, key, valueTimestamp, valueTombstoned, rowPointer);
                                rebuilt.increment();
                                return true;
                            });
                        }
                        return true;
                    });
                }, null);

                LOG.info("Rebuilt ({}) {} for {}.", rebuilt.longValue(), walIndex.getClass().getSimpleName(), versionedPartitionName);
                walIndex.commit();
            } else {
                LOG.info("Checking {} for {}.", walIndex.getClass().getSimpleName(), versionedPartitionName);

                final MutableLong repair = new MutableLong();
                walIndex.merge((TxKeyPointerStream stream) -> {
                    return io.reverseScan(new RowStream() {
                        long commitedUpToTxId = Long.MIN_VALUE;

                        @Override
                        public boolean row(long rowFP, long rowTxId, RowType rowType, byte[] row) throws Exception {
                            if (rowType == RowType.primary) {
                                primaryRowMarshaller.fromRow(row, (key, value, valueTimestamp, valueTombstoned) -> {
                                    stream.stream(rowTxId, key, valueTimestamp, valueTombstoned, rowFP);
                                    repair.add(1);
                                    return true;
                                });

                            }
                            if (rowType == RowType.system && commitedUpToTxId == Long.MIN_VALUE) {
                                long[] key_CommitedUpToTxId = UIO.bytesLongs(row);
                                if (key_CommitedUpToTxId[0] == RowType.COMMIT_KEY) {
                                    commitedUpToTxId = key_CommitedUpToTxId[1];
                                }
                                return true;
                            } else {
                                return rowTxId >= commitedUpToTxId;
                            }
                        }
                    });
                }, null);

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
    public Optional<Compacted> compact(final long removeTombstonedOlderThanTimestampId,
        long ttlTimestampId,
        WALIndex rowIndex) throws Exception {

        long endOfLastRow = io.getEndOfLastRow();
        if (lastEndOfLastRow.get() == -1) {
            lastEndOfLastRow.set(endOfLastRow);
            return Optional.absent();
        }
        if (compactAfterGrowthFactor > -1 && endOfLastRow < lastEndOfLastRow.get() * compactAfterGrowthFactor) {
            return Optional.absent();
        }
        lastEndOfLastRow.set(endOfLastRow);
        long start = System.currentTimeMillis();

        WALIndex.CompactionWALIndex compactionRowIndex = rowIndex != null ? rowIndex.startCompaction() : null;

        File tempDir = Files.createTempDir();
        RowIO compactionIO = ioProvider.create(tempDir, name);

        AtomicLong keyCount = new AtomicLong();
        AtomicLong removeCount = new AtomicLong();
        AtomicLong tombstoneCount = new AtomicLong();
        AtomicLong ttlCount = new AtomicLong();

        compact(0,
            endOfLastRow,
            rowIndex,
            compactionRowIndex,
            compactionIO,
            keyCount,
            removeCount,
            tombstoneCount,
            ttlCount,
            removeTombstonedOlderThanTimestampId,
            ttlTimestampId);

        return Optional.<Compacted>of((Compacted) () -> {
            compactionLock.acquire(NUM_PERMITS);
            try {
                long startCatchup = System.currentTimeMillis();
                AtomicLong catchupKeys = new AtomicLong();
                AtomicLong catchupRemoves = new AtomicLong();
                AtomicLong catchupTombstones = new AtomicLong();
                AtomicLong catchupTTL = new AtomicLong();
                compact(endOfLastRow, Long.MAX_VALUE, rowIndex, compactionRowIndex, compactionIO,
                    catchupKeys, catchupRemoves, catchupTombstones, catchupTTL,
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
                long endOfLastRow1 = io.getEndOfLastRow();
                lastEndOfLastRow.set(endOfLastRow1);
                return new CommittedCompacted(rowIndex, sizeBeforeCompaction, sizeAfterCompaction, keyCount.longValue(), removeCount.longValue(),
                    tombstoneCount.longValue(), ttlCount.longValue(), System.currentTimeMillis() - start, catchupKeys.longValue(),
                    catchupRemoves.longValue(), catchupTombstones.longValue(), catchupTTL.longValue(), System.currentTimeMillis() - startCatchup);
            } finally {
                compactionLock.release(NUM_PERMITS);
            }
        });

    }

    private void compact(long startAtRow,
        long endOfLastRow,
        WALIndex rowIndex,
        CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        AtomicLong keyCount,
        AtomicLong removeCount,
        AtomicLong tombstoneCount,
        AtomicLong ttlCount,
        long removeTombstonedOlderThanTimestampId,
        long ttlTimestampId) throws
        Exception {

        Preconditions.checkNotNull(rowIndex, "If you don't have one use NOOpWALIndex.");

        List<CompactionFlushable> flushables = new ArrayList<>();
        MutableLong flushTxId = new MutableLong(-1);
        io.scan(startAtRow, false, (final long rowFP, long rowTxId, RowType rowType, final byte[] row) -> {
            if (rowFP >= endOfLastRow) {
                return false;
            }
            if (rowType.isDiscardedDuringCompactions()) {
                return true;
            }

            long lastTxId = flushTxId.longValue();
            if (lastTxId != rowTxId) {
                flushBatch(compactionWALIndex, compactionIO, flushables, lastTxId);
                flushTxId.setValue(rowTxId);
            }

            if (rowType == RowType.primary) {
                primaryRowMarshaller.fromRow(row, (key, value, valueTimestamp, valueTombstoned) -> {
                    rowIndex.getPointer(key, (key1, pointerTimestamp, pointerTombstoned, pointerFp) -> {
                        if (pointerFp == -1 || valueTimestamp >= pointerTimestamp) {
                            if (valueTombstoned && valueTimestamp < removeTombstonedOlderThanTimestampId) {
                                tombstoneCount.incrementAndGet();
                            } else {
                                if (valueTimestamp > ttlTimestampId) {
                                    flushables.add(new CompactionFlushable(key, valueTimestamp, valueTombstoned, row));
                                    keyCount.incrementAndGet();
                                } else {
                                    ttlCount.incrementAndGet();
                                }
                            }
                        } else {
                            removeCount.incrementAndGet();
                        }
                        return true;
                    });
                    return true;
                });

            } else if (rowType == RowType.highwater) {
                compactionIO.writeHighwater(row);
            } else {
                // system is ignored
            }
            return true;
        });
        flushBatch(compactionWALIndex, compactionIO, flushables, flushTxId.longValue());
    }

    private void flushBatch(CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        List<CompactionFlushable> flushables,
        long lastTxId) throws Exception {

        if (flushables.isEmpty()) {
            return;
        }
        flush(compactionWALIndex, compactionIO, lastTxId, RowType.primary,
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
                    if (!indexableKeyStream.stream(flushable.key, flushable.valueTimestamp, flushable.valueTombstoned)) {
                        return false;
                    }
                }
                return true;
            });
        flushables.clear();
    }

    private void flush(CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        long txId,
        RowType rowType,
        RawRows rows,
        IndexableKeys indexableKeys) throws Exception {

        if (compactionWALIndex != null) {
            compactionWALIndex.merge((TxKeyPointerStream stream) -> {
                compactionIO.write(txId, rowType, rows, indexableKeys, (rowTxId, key, valueTimestamp, valueTombstoned, fp) -> {
                    return stream.stream(txId, key, valueTimestamp, valueTombstoned, fp);
                });
                return true;
            });
        } else {
            compactionIO.write(txId, rowType, rows, indexableKeys, (rowTxId, key, valueTimestamp, valueTombstoned, fp) -> true);
        }
    }

    public static class CompactionFlushable {
        public byte[] key;
        public long valueTimestamp;
        public boolean valueTombstoned;
        public byte[] row;

        public CompactionFlushable(byte[] key, long valueTimestamp, boolean valueTombstoned, byte[] row) {
            this.key = key;
            this.valueTimestamp = valueTimestamp;
            this.valueTombstoned = valueTombstoned;
            this.row = row;
        }
    }
}
