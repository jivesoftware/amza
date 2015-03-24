package com.jivesoftware.os.amza.storage.binary;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALIndex.CompactionWALIndex;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.filer.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.io.File;
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
public class BinaryRowsTx implements WALTx {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int numPermits = 1024;
    private final Semaphore compactionLock = new Semaphore(numPermits, true);
    private final File dir;
    private final String name;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final WALIndexProvider rowsIndexProvider;
    private final int backRepairIndexForNDistinctTxIds;
    private final AtomicLong lastEndOfLastRow = new AtomicLong(-1);

    private final RowIOProvider ioProvider;
    private RowIO io;

    public BinaryRowsTx(File dir,
        String name,
        RowIOProvider ioProvider,
        RowMarshaller<byte[]> rowMarshaller,
        WALIndexProvider rowsIndexProvider,
        int backRepairIndexForNDistinctTxIds) throws Exception {
        this.dir = dir;
        this.name = name;
        this.ioProvider = ioProvider;
        this.rowMarshaller = rowMarshaller;
        this.rowsIndexProvider = rowsIndexProvider;
        this.backRepairIndexForNDistinctTxIds = backRepairIndexForNDistinctTxIds;
        io = ioProvider.create(dir, name);
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
    public WALIndex load(RegionName regionName) throws Exception {
        compactionLock.acquire(numPermits);
        try {
            final WALIndex rowsIndex = rowsIndexProvider.createIndex(regionName);
            if (rowsIndex.isEmpty()) {
                LOG.info(
                    "Loading rowIndex:" + rowsIndex.getClass().getSimpleName()
                    + " region:" + regionName.getRegionName() + "-" + regionName.getRingName() + "...");
                io.scan(0, new WALReader.Stream() {
                    @Override
                    public boolean row(final long rowPointer, byte rowType, byte[] row) throws Exception {
                        RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                        WALKey key = walr.getKey();
                        WALValue value = walr.getValue();
                        WALValue current = rowsIndex.get(Collections.singletonList(key)).get(0);
                        if (current == null) {
                            rowsIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                                key, new WALValue(UIO.longBytes(rowPointer), value.getTimestampId(), value.getTombstoned()))));
                        } else if (current.getTimestampId() < value.getTimestampId()) {
                            rowsIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                                key, new WALValue(UIO.longBytes(rowPointer), value.getTimestampId(), value.getTombstoned()))));
                        }
                        return true;
                    }
                });
                LOG.info("Loaded rowIndex:" + rowsIndex.getClass().getSimpleName()
                    + " region:" + regionName.getRegionName() + "-" + regionName.getRingName() + ".");
            } else {
                LOG.info("Checking rowIndex:" + rowsIndex.getClass().getSimpleName()
                    + " region:" + regionName.getRegionName() + "-" + regionName.getRingName() + ".");
                final Set<Long> txIds = new HashSet<>();
                io.reverseScan(new WALReader.Stream() {

                    @Override
                    public boolean row(long rowPointer, byte rowType, byte[] row) throws Exception {
                        RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                        WALKey key = walr.getKey();
                        WALValue value = walr.getValue();
                        rowsIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                            key, new WALValue(UIO.longBytes(rowPointer), value.getTimestampId(), value.getTombstoned()))));
                        txIds.add(walr.getTransactionId());
                        return txIds.size() < backRepairIndexForNDistinctTxIds;
                    }
                });
                LOG.info("Checked rowIndex:" + rowsIndex.getClass().getSimpleName()
                    + " region:" + regionName.getRegionName() + "-" + regionName.getRingName() + ".");
            }
            return rowsIndex;
        } finally {
            compactionLock.release(numPermits);
        }
    }

    @Override
    public Optional<Compacted> compact(final RegionName regionName, final long removeTombstonedOlderThanTimestampId, final WALIndex rowIndex) throws Exception {

        final String metricPrefix = "region>" + regionName.getRegionName() + ">ring>" + regionName.getRingName() + ">";
        LOG.inc(metricPrefix + "checks");
        final long endOfLastRow = io.getEndOfLastRow();
        if (lastEndOfLastRow.get() == -1) {
            lastEndOfLastRow.set(endOfLastRow);
            return Optional.absent();
        }
        if (endOfLastRow < lastEndOfLastRow.get() * 2) {
            return Optional.absent();
        }
        LOG.info("Compacting region:" + regionName + "...");
        lastEndOfLastRow.set(endOfLastRow);
        final long start = System.currentTimeMillis();

        LOG.inc(metricPrefix + "compacted");

        final WALIndex.CompactionWALIndex compactionRowIndex = rowIndex != null ? rowIndex.startCompaction() : null;

        File tempDir = Files.createTempDir();
        final RowIO compactionIO = ioProvider.create(tempDir, name);

        final AtomicLong keyCount = new AtomicLong();
        final AtomicLong removeCount = new AtomicLong();
        final AtomicLong tombstoneCount = new AtomicLong();

        compact(0, endOfLastRow, rowIndex, compactionRowIndex, compactionIO, keyCount, removeCount, tombstoneCount, removeTombstonedOlderThanTimestampId);

        return Optional.<Compacted>of(new Compacted() {

            @Override
            public WALIndex getCompactedWALIndex() throws Exception {

                compactionLock.acquire(numPermits);
                try {
                    long startCatchup = System.currentTimeMillis();
                    AtomicLong catchupKeys = new AtomicLong();
                    AtomicLong catchupRemoves = new AtomicLong();
                    AtomicLong catchupTombstones = new AtomicLong();
                    compact(endOfLastRow, Long.MAX_VALUE, rowIndex, compactionRowIndex, compactionIO,
                        catchupKeys, catchupRemoves, catchupTombstones,
                        removeTombstonedOlderThanTimestampId);
                    compactionIO.flush();
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

                    LOG.info("Compacted region " + dir.getAbsolutePath() + "/" + name
                        + " was:" + sizeBeforeCompaction + "bytes "
                        + " isNow:" + sizeAfterCompaction + "bytes.");
                    compactionRowIndex.commit();

                    long endOfLastRow = io.getEndOfLastRow();
                    lastEndOfLastRow.set(endOfLastRow);
                    return rowIndex;
                } finally {
                    compactionLock.release(numPermits);
                }
            }
        });

    }

    private void compact(long startAtRow,
        final long endOfLastRow,
        final WALIndex rowIndex,
        final CompactionWALIndex compactedRowsIndex,
        final RowIO compactionIO,
        final AtomicLong keyCount,
        final AtomicLong removeCount,
        final AtomicLong tombstoneCount,
        final long removeTombstonedOlderThanTimestampId) throws
        Exception {

        final List<WALKey> rowKeys = new ArrayList<>();
        final List<WALValue> rowValues = new ArrayList<>();
        final List<Byte> rawRowTypes = new ArrayList<>();
        final List<byte[]> rawRows = new ArrayList<>();
        final AtomicLong batchSizeInBytes = new AtomicLong();

        io.scan(startAtRow, new WALReader.Stream() {
            @Override
            public boolean row(final long rowPointer, byte rowType, final byte[] row) throws Exception {
                if (rowPointer >= endOfLastRow) {
                    return false;
                }
                if (rowType < 0) { // TODO expose to caller which rowtypes they want to preserve. For now we discard all system rowtypes and keep all others.
                    return true;
                }

                RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                WALKey key = walr.getKey();
                WALValue value = walr.getValue();

                WALValue got = rowIndex == null ? null : rowIndex.get(Collections.singletonList(key)).get(0);
                if (got == null || value.getTimestampId() >= got.getTimestampId()) {
                    if (value.getTombstoned() && value.getTimestampId() < removeTombstonedOlderThanTimestampId) {
                        tombstoneCount.incrementAndGet();
                    } else {
                        rowKeys.add(key);
                        rowValues.add(value);
                        rawRowTypes.add(rowType);
                        rawRows.add(row);
                        batchSizeInBytes.addAndGet(row.length);
                        keyCount.incrementAndGet();
                    }
                } else {
                    removeCount.incrementAndGet();
                }
                long batchingSize = 1024 * 1024 * 10; // TODO expose to config
                if (batchSizeInBytes.get() > batchingSize) {
                    flush(compactedRowsIndex, compactionIO, rawRowTypes, rawRows, rowKeys, rowValues, batchSizeInBytes);
                }
                return true;
            }

        });
        if (!rawRows.isEmpty()) {
            flush(compactedRowsIndex, compactionIO, rawRowTypes, rawRows, rowKeys, rowValues, batchSizeInBytes);
        }
    }

    private void flush(CompactionWALIndex compactedRowsIndex,
        RowIO compactionIO,
        List<Byte> rawRowTypes,
        List<byte[]> rawRows,
        List<WALKey> rowKeys,
        List<WALValue> rowValues,
        AtomicLong batchSizeInBytes) throws Exception {

        List<byte[]> rowPointers = compactionIO.write(rawRowTypes, rawRows, true);
        Collection<Map.Entry<WALKey, WALValue>> entries = new ArrayList<>(rowKeys.size());
        for (int i = 0; i < rowKeys.size(); i++) {
            WALValue rowValue = rowValues.get(i);
            entries.add(new AbstractMap.SimpleEntry<>(rowKeys.get(i),
                new WALValue(rowPointers.get(i), rowValue.getTimestampId(), rowValue.getTombstoned())));
        }
        if (compactedRowsIndex != null) {
            compactedRowsIndex.put(entries);
        }
        rowKeys.clear();
        rowValues.clear();
        rawRows.clear();
        batchSizeInBytes.set(0);

    }
}
