package com.jivesoftware.os.amza.storage.binary;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowReader;
import com.jivesoftware.os.amza.shared.RowsIndex;
import com.jivesoftware.os.amza.shared.RowsIndex.CompactionRowIndex;
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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final int backRepairIndexForNDistinctTxIds;
    private final AtomicLong lastEndOfLastRow = new AtomicLong(-1);

    private IFiler filer;
    private BinaryRowReader rowReader;
    private BinaryRowWriter rowWriter;

    public BinaryRowsTx(File tableFile,
        RowMarshaller<byte[]> rowMarshaller,
        RowsIndexProvider tableIndexProvider,
        int backRepairIndexForNDistinctTxIds) throws IOException {
        this.tableFile = tableFile;
        this.rowMarshaller = rowMarshaller;
        this.tableIndexProvider = tableIndexProvider;
        this.backRepairIndexForNDistinctTxIds = backRepairIndexForNDistinctTxIds;
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
            if (rowsIndex.isEmpty()) {
                LOG.info(
                    "Loading rowIndex:" + rowsIndex.getClass().getSimpleName()
                    + " table:" + tableName.getTableName() + "-" + tableName.getRingName() + "...");
                rowReader.scan(0, new RowReader.Stream<byte[]>() {
                    @Override
                    public boolean row(final long rowPointer, byte[] row) throws Exception {
                        RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                        RowIndexKey key = walr.getKey();
                        RowIndexValue value = walr.getValue();
                        RowIndexValue current = rowsIndex.get(Collections.singletonList(key)).get(0);
                        if (current == null) {
                            rowsIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                                key, new RowIndexValue(UIO.longBytes(rowPointer), value.getTimestampId(), value.getTombstoned()))));
                        } else if (current.getTimestampId() < value.getTimestampId()) {
                            rowsIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                                key, new RowIndexValue(UIO.longBytes(rowPointer), value.getTimestampId(), value.getTombstoned()))));
                        }
                        return true;
                    }
                });
                LOG.info("Loaded rowIndex:" + rowsIndex.getClass().getSimpleName()
                    + " table:" + tableName.getTableName() + "-" + tableName.getRingName() + ".");
            } else {
                LOG.info("Checking rowIndex:" + rowsIndex.getClass().getSimpleName()
                    + " table:" + tableName.getTableName() + "-" + tableName.getRingName() + ".");
                final Set<Long> txIds = new HashSet<>();
                rowReader.reverseScan(new RowReader.Stream<byte[]>() {

                    @Override
                    public boolean row(long rowPointer, byte[] row) throws Exception {
                        RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                        RowIndexKey key = walr.getKey();
                        RowIndexValue value = walr.getValue();
                        rowsIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                            key, new RowIndexValue(UIO.longBytes(rowPointer), value.getTimestampId(), value.getTombstoned()))));
                        txIds.add(walr.getTransactionId());
                        return txIds.size() < backRepairIndexForNDistinctTxIds;
                    }
                });
                LOG.info("Checked rowIndex:" + rowsIndex.getClass().getSimpleName()
                    + " table:" + tableName.getTableName() + "-" + tableName.getRingName() + ".");
            }
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
        if (lastEndOfLastRow.get() == -1) {
            lastEndOfLastRow.set(endOfLastRow);
            return Optional.absent();
        }
        if (endOfLastRow < lastEndOfLastRow.get() * 2) {
            return Optional.absent();
        }
        LOG.info("Compacting table:" + tableName + "...");
        lastEndOfLastRow.set(endOfLastRow);
        final long start = System.currentTimeMillis();

        LOG.inc(metricPrefix + "compacted");

        final RowsIndex.CompactionRowIndex compactionRowIndex = rowIndex.startCompaction();

        File dir = Files.createTempDir();
        String name = tableFile.getName();
        final File compactTo = new File(dir, name);
        final Filer compactionFiler = new Filer(compactTo.getAbsolutePath(), "rw");
        final BinaryRowWriter compactionWriter = new BinaryRowWriter(compactionFiler);
        final AtomicLong keyCount = new AtomicLong();
        final AtomicLong removeCount = new AtomicLong();
        final AtomicLong tombstoneCount = new AtomicLong();

        compact(0, endOfLastRow, rowIndex, compactionRowIndex, compactionWriter, keyCount, removeCount, tombstoneCount, removeTombstonedOlderThanTimestampId);

        return Optional.<Compacted>of(new Compacted() {

            @Override
            public RowsIndex getCompactedRowsIndex() throws Exception {

                compactionLock.acquire(numPermits);
                try {
                    long startCatchup = System.currentTimeMillis();
                    AtomicLong catchupKeys = new AtomicLong();
                    AtomicLong catchupRemoves = new AtomicLong();
                    AtomicLong catchupTombstones = new AtomicLong();
                    compact(endOfLastRow, Long.MAX_VALUE, rowIndex, compactionRowIndex, compactionWriter,
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

                    LOG.info("Compacted table " + tableFile.getAbsolutePath()
                        + " was:" + sizeBeforeCompaction + "bytes "
                        + " isNow:" + sizeAfterCompaction + "bytes.");
                    compactionRowIndex.commit();

                    long endOfLastRow = rowWriter.getEndOfLastRow();
                    lastEndOfLastRow.set(endOfLastRow);
                    return rowIndex;
                } finally {
                    compactionLock.release(numPermits);
                }
            }
        });

    }

    private void compact(long startAtRow, final long endOfLastRow,
        final RowsIndex rowIndex,
        final CompactionRowIndex compactedRowsIndex,
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
                final List<RowIndexKey> rowKeys = new ArrayList<>();
                final List<RowIndexValue> rowValues = new ArrayList<>();
                final List<byte[]> rawRows = new ArrayList<>();
                final AtomicLong batchSizeInBytes = new AtomicLong();

                RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                RowIndexKey key = walr.getKey();
                RowIndexValue value = walr.getValue();

                RowIndexValue got = rowIndex.get(Collections.singletonList(key)).get(0);
                if (got == null || value.getTimestampId() >= got.getTimestampId()) {
                    if (value.getTombstoned() && value.getTimestampId() < removeTombstonedOlderThanTimestampId) {
                        tombstoneCount.incrementAndGet();
                    } else {
                        rowKeys.add(key);
                        rowValues.add(value);
                        rawRows.add(row);
                        batchSizeInBytes.addAndGet(row.length);
                        keyCount.incrementAndGet();
                    }
                } else {
                    removeCount.incrementAndGet();
                }
                long batchingSize = 1024 * 1024 * 10; // TODO expose to config
                if (batchSizeInBytes.get() > batchingSize) {
                    flush(rawRows, rowKeys, rowValues, batchSizeInBytes);
                }

                if (!rawRows.isEmpty()) {
                    flush(rawRows, rowKeys, rowValues, batchSizeInBytes);
                }
                return true;
            }

            private void flush(final List<byte[]> rawRows, final List<RowIndexKey> rowKeys, final List<RowIndexValue> rowValues,
                final AtomicLong batchSizeInBytes) throws Exception {

                List<byte[]> rowPointers = compactionWriter.write(rawRows, true);
                Collection<Map.Entry<RowIndexKey, RowIndexValue>> entries = new ArrayList<>(rowKeys.size());
                for (int i = 0; i < rowKeys.size(); i++) {
                    RowIndexValue rowValue = rowValues.get(i);
                    entries.add(new AbstractMap.SimpleEntry<>(rowKeys.get(i),
                        new RowIndexValue(rowPointers.get(i), rowValue.getTimestampId(), rowValue.getTombstoned())));
                }
                compactedRowsIndex.put(entries);
                rowKeys.clear();
                rowValues.clear();
                rawRows.clear();
                batchSizeInBytes.set(0);
            }
        });
    }
}
