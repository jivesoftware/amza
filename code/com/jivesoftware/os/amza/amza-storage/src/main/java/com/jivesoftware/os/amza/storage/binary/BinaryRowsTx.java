package com.jivesoftware.os.amza.storage.binary;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowReader;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowsIndex;
import com.jivesoftware.os.amza.shared.RowsIndexProvider;
import com.jivesoftware.os.amza.shared.RowsTx;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.filer.Filer;
import com.jivesoftware.os.amza.storage.filer.IFiler;
import com.jivesoftware.os.amza.storage.filer.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class BinaryRowsTx implements RowsTx<byte[]> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int numPermits = 1024;
    private final Semaphore compactionLock = new Semaphore(numPermits, true);
    private final File tableFile;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final RowsIndexProvider tableIndexProvider;
    private final AtomicLong lastEndOfLastRow = new AtomicLong(-1);

    private IFiler filer;
    private BinaryRowReader rowReader;
    private BinaryRowWriter rowWriter;

    public BinaryRowsTx(File tableFile,
        RowMarshaller<byte[]> rowMarshaller,
        RowsIndexProvider tableIndexProvider) throws IOException {
        this.tableFile = tableFile;
        this.rowMarshaller = rowMarshaller;
        this.tableIndexProvider = tableIndexProvider;
        filer = new Filer(tableFile.getAbsolutePath(), "rw");
        rowReader = new BinaryRowReader(filer);
        rowWriter = new BinaryRowWriter(filer);
    }

    @Override
    public <R> R write(RowsWrite<byte[], R> write) throws Exception {
        compactionLock.acquire();
        try {
            return write.write(rowWriter);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public <R> R read(RowsRead<byte[], R> read) throws Exception {
        compactionLock.acquire();
        try {
            return read.read(rowReader);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public RowsIndex load(TableName tableName) throws Exception {
        compactionLock.acquire(numPermits);
        try {
            final RowsIndex rowsIndex = tableIndexProvider.createRowsIndex(tableName);
            rowReader.scan(0, new RowReader.Stream<byte[]>() {
                @Override
                public boolean row(final long rowPointer, byte[] row) throws Exception {
                    rowMarshaller.fromRow(row, new RowScan() {

                        @Override
                        public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
                            RowIndexValue current = rowsIndex.get(key);
                            if (current == null) {
                                rowsIndex.put(key, new RowIndexValue(UIO.longBytes(rowPointer), value.getTimestampId(), value.getTombstoned()));
                            } else if (current.getTimestampId() < value.getTimestampId()) {
                                rowsIndex.put(key, new RowIndexValue(UIO.longBytes(rowPointer), value.getTimestampId(), value.getTombstoned()));
                            }
                            return true;
                        }
                    });
                    return true;
                }
            });
            return rowsIndex;
        } finally {
            compactionLock.release(numPermits);
        }
    }

    @Override
    public Optional<Compacted> compact(final TableName tableName, final long removeTombstonedOlderThanTimestampId, final RowsIndex rowIndex) throws Exception {

        final String metricPrefix = "tables>" + tableName.getTableName() + ">ring>" + tableName.getRingName() + ">";
        LOG.inc(metricPrefix + "checks");
        final long endOfLastRow = rowWriter.getEndOfLastRow();
        if (lastEndOfLastRow.get() != -1 && endOfLastRow < lastEndOfLastRow.get() * 2) {
            return Optional.absent();
        }
        lastEndOfLastRow.set(endOfLastRow);
        final long start = System.currentTimeMillis();

        LOG.inc(metricPrefix + "compacted");

        final RowsIndex compactedRowsIndex = tableIndexProvider.createRowsIndex(tableName);

        File dir = Files.createTempDir();
        String name = tableFile.getName();
        final File compactTo = new File(dir, name);
        final Filer compactionFiler = new Filer(compactTo.getAbsolutePath(), "rw");
        final BinaryRowWriter compactionWriter = new BinaryRowWriter(compactionFiler);
        final AtomicLong keyCount = new AtomicLong();
        final AtomicLong removeCount = new AtomicLong();
        final AtomicLong tombstoneCount = new AtomicLong();

        compact(0, endOfLastRow, rowIndex, compactedRowsIndex, compactionWriter, keyCount, removeCount, tombstoneCount, removeTombstonedOlderThanTimestampId);

        return Optional.<Compacted>of(new Compacted() {

            @Override
            public RowsIndex getCompactedRowsIndex() throws Exception {

                compactionLock.acquire(numPermits);
                try {
                    long startCatchup = System.currentTimeMillis();
                    AtomicLong catchupKeys = new AtomicLong();
                    AtomicLong catchupRemoves = new AtomicLong();
                    AtomicLong catchupTombstones = new AtomicLong();
                    compact(endOfLastRow, Long.MAX_VALUE, rowIndex, compactedRowsIndex, compactionWriter,
                        catchupKeys, catchupRemoves, catchupTombstones,
                        removeTombstonedOlderThanTimestampId);
                    compactionFiler.flush();
                    compactionFiler.close();

                    File backup = new File(tableFile.getParentFile(), "bkp-" + tableFile.getName());
                    backup.delete();

                    long sizeBeforeCompaction = tableFile.length();
                    long sizeAfterCompaction = compactTo.length();
                    Files.move(tableFile, backup);
                    Files.move(compactTo, tableFile);

                    // Reopen the world
                    filer.close();
                    filer = new Filer(tableFile.getAbsolutePath(), "rw"); // HACKY as HELL
                    rowReader = new BinaryRowReader(filer); // HACKY as HELL
                    rowWriter = new BinaryRowWriter(filer); // HACKY as HELL

                    System.out.println("Compacted table " + tableFile.getAbsolutePath()
                        + " was:" + sizeBeforeCompaction + "bytes "
                        + " isNow:" + sizeAfterCompaction + "bytes.");

                    LOG.set(ValueType.COUNT, metricPrefix + "sizeBeforeCompaction", sizeBeforeCompaction);
                    LOG.set(ValueType.COUNT, metricPrefix + "sizeAfterCompaction", sizeAfterCompaction);
                    LOG.set(ValueType.COUNT, metricPrefix + "keeps", keyCount.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "removes", removeCount.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "tombstones", tombstoneCount.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "duration", System.currentTimeMillis() - start);
                    LOG.set(ValueType.COUNT, metricPrefix + "catchupKeeps", catchupKeys.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "catchupRemoves", catchupRemoves.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "catchupTombstones", catchupTombstones.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "catchupDuration", System.currentTimeMillis() - startCatchup);

                    return compactedRowsIndex;
                } finally {
                    compactionLock.release(numPermits);
                }
            }
        });

    }

    private void compact(long startAtRow, final long endOfLastRow,
        final RowsIndex rowIndex,
        final RowsIndex compactedRowsIndex,
        final BinaryRowWriter compactionWriter,
        final AtomicLong keyCount,
        final AtomicLong removeCount,
        final AtomicLong tombstoneCount,
        final long removeTombstonedOlderThanTimestampId) throws
        Exception {
        rowReader.scan(startAtRow, new RowReader.Stream<byte[]>() {
            @Override
            public boolean row(final long rowPointer, final byte[] row) throws Exception {
                if (rowPointer >= endOfLastRow) {
                    return false;
                }
                final List<byte[]> rows = new ArrayList<>();
                final AtomicLong batchSizeInBytes = new AtomicLong();
                rowMarshaller.fromRow(row, new RowScan() {

                    @Override
                    public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {

                        RowIndexValue got = rowIndex.get(key);
                        if (got == null || value.getTimestampId() >= got.getTimestampId()) {
                            if (value.getTombstoned() && value.getTimestampId() < removeTombstonedOlderThanTimestampId) {
                                tombstoneCount.incrementAndGet();
                            } else {
                                rows.add(row);
                                batchSizeInBytes.addAndGet(row.length);
                                compactedRowsIndex.put(key, value);
                                keyCount.incrementAndGet();
                            }
                        } else {
                            removeCount.incrementAndGet();
                        }
                        long batchingSize = 1024 * 1024 * 10; // TODO expose to config
                        if (batchSizeInBytes.get() > batchingSize) {
                            compactionWriter.write(rows, true);
                            rows.clear();
                            batchSizeInBytes.set(0);
                        }
                        return true;
                    }
                });
                if (!rows.isEmpty()) {
                    compactionWriter.write(rows, true);
                }
                return true;
            }
        });
    }

}
