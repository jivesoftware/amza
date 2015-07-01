package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.AmzaVersionConstants;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALIndex.CompactionWALIndex;
import com.jivesoftware.os.amza.shared.wal.WALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
import com.jivesoftware.os.amza.shared.wal.WALRow;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    private RowIO io;

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

                final MutableLong rebuilt = new MutableLong();
                io.scan(0, true, (final long rowPointer, long rowTxId, RowType rowType, byte[] row) -> {
                    if (rowType == RowType.primary) {
                        WALRow walr = primaryRowMarshaller.fromRow(row);
                        WALKey key = walr.key;
                        WALValue value = walr.value;
                        WALPointer current = walIndex.getPointer(key);
                        if (current == null) {
                            walIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                                key, new WALPointer(rowPointer, value.getTimestampId(), value.getTombstoned()))));
                        } else if (current.getTimestampId() < value.getTimestampId()) {
                            walIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                                key, new WALPointer(rowPointer, value.getTimestampId(), value.getTombstoned()))));
                        }
                        rebuilt.add(1);
                    }
                    return true;
                });
                LOG.info("Rebuilt ({}) {} for {}.", rebuilt.longValue(), walIndex.getClass().getSimpleName(), versionedPartitionName);
                walIndex.commit();
            } else {
                LOG.info("Checking {} for {}.", walIndex.getClass().getSimpleName(), versionedPartitionName);
                final MutableLong repair = new MutableLong();
                io.reverseScan(new RowStream() {
                    long commitedUpToTxId = Long.MIN_VALUE;

                    @Override
                    public boolean row(long rowFP, long rowTxId, RowType rowType, byte[] row) throws Exception {
                        if (rowType == RowType.primary) {
                            WALRow walr = primaryRowMarshaller.fromRow(row);
                            WALKey key = walr.key;
                            WALValue value = walr.value;
                            walIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                                key, new WALPointer(rowFP, value.getTimestampId(), value.getTombstoned()))));
                            repair.add(1);
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
        final long ttlTimestampId,
        final WALIndex rowIndex) throws Exception {

        final long endOfLastRow = io.getEndOfLastRow();
        if (lastEndOfLastRow.get() == -1) {
            lastEndOfLastRow.set(endOfLastRow);
            return Optional.absent();
        }
        if (compactAfterGrowthFactor > -1 && endOfLastRow < lastEndOfLastRow.get() * compactAfterGrowthFactor) {
            return Optional.absent();
        }
        lastEndOfLastRow.set(endOfLastRow);
        final long start = System.currentTimeMillis();

        final WALIndex.CompactionWALIndex compactionRowIndex = rowIndex != null ? rowIndex.startCompaction() : null;

        File tempDir = Files.createTempDir();
        final RowIO compactionIO = ioProvider.create(tempDir, name);

        final AtomicLong keyCount = new AtomicLong();
        final AtomicLong removeCount = new AtomicLong();
        final AtomicLong tombstoneCount = new AtomicLong();
        final AtomicLong ttlCount = new AtomicLong();

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
                backup.mkdirs();
                long sizeBeforeCompaction = io.sizeInBytes();
                io.close();
                io.move(backup);
                dir.mkdirs();
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
        final long endOfLastRow,
        final WALIndex rowIndex,
        final CompactionWALIndex compactionWALIndex,
        final RowIO compactionIO,
        final AtomicLong keyCount,
        final AtomicLong removeCount,
        final AtomicLong tombstoneCount,
        final AtomicLong ttlCount,
        final long removeTombstonedOlderThanTimestampId,
        final long ttlTimestampId) throws
        Exception {

        final List<WALKey> rowKeys = new ArrayList<>();
        final List<WALValue> rowValues = new ArrayList<>();
        final List<Long> rowTxIds = new ArrayList<>();
        final List<RowType> rawRowTypes = new ArrayList<>();
        final List<byte[]> rawRows = new ArrayList<>();
        final AtomicLong batchSizeInBytes = new AtomicLong();

        io.scan(startAtRow, false, (final long rowFP, long rowTxId, RowType rowType, final byte[] row) -> {
            if (rowFP >= endOfLastRow) {
                return false;
            }
            if (rowType.isDiscardedDuringCompactions()) {
                return true;
            }

            if (rowType == RowType.primary) {
                WALRow walr = primaryRowMarshaller.fromRow(row);
                WALKey key = walr.key;
                WALValue value = walr.value;

                WALPointer got = (rowIndex == null) ? null : rowIndex.getPointer(key);
                if (got == null || value.getTimestampId() >= got.getTimestampId()) {
                    if (value.getTombstoned() && value.getTimestampId() < removeTombstonedOlderThanTimestampId) {
                        tombstoneCount.incrementAndGet();
                    } else {
                        if (value.getTimestampId() > ttlTimestampId) {
                            rowKeys.add(key);
                            rowValues.add(value);
                            rowTxIds.add(rowTxId);
                            rawRowTypes.add(rowType);
                            rawRows.add(row);
                            batchSizeInBytes.addAndGet(row.length);
                            keyCount.incrementAndGet();
                        } else {
                            ttlCount.incrementAndGet();
                        }
                    }
                } else {
                    removeCount.incrementAndGet();
                }
            } else if (rowType == RowType.highwater) {
                flush(compactionWALIndex, compactionIO, rowTxIds, rawRowTypes, rawRows, rowKeys, rowValues, batchSizeInBytes);
                compactionIO.writeHighwater(row);
            } else {
                long batchingSize = 1024 * 1024 * 10; // TODO expose to config
                if (batchSizeInBytes.get() > batchingSize) {
                    flush(compactionWALIndex, compactionIO, rowTxIds, rawRowTypes, rawRows, rowKeys, rowValues, batchSizeInBytes);
                }
            }
            return true;
        });
        if (!rawRows.isEmpty()) {
            flush(compactionWALIndex, compactionIO, rowTxIds, rawRowTypes, rawRows, rowKeys, rowValues, batchSizeInBytes);
        }
    }

    private void flush(CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        List<Long> rowTxIds,
        List<RowType> rawRowTypes,
        List<byte[]> rawRows,
        List<WALKey> rowKeys,
        List<WALValue> rowValues,
        AtomicLong batchSizeInBytes) throws Exception {

        long[] rowPointers = compactionIO.write(rowTxIds, rawRowTypes, rawRows);
        Collection<Map.Entry<WALKey, WALPointer>> entries = new ArrayList<>(rowKeys.size());
        for (int i = 0; i < rowKeys.size(); i++) {
            WALValue rowValue = rowValues.get(i);
            entries.add(new AbstractMap.SimpleEntry<>(rowKeys.get(i),
                new WALPointer(rowPointers[i], rowValue.getTimestampId(), rowValue.getTombstoned())));
        }
        if (compactionWALIndex != null) {
            compactionWALIndex.put(entries);
        }
        rowKeys.clear();
        rowValues.clear();
        rawRows.clear();
        batchSizeInBytes.set(0);

    }
}
