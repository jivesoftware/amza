package com.jivesoftware.os.amza.storage.binary;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.AmzaVersionConstants;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALIndex.CompactionWALIndex;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
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
import org.apache.commons.io.FileUtils;
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
    private final RowMarshaller<byte[]> rowMarshaller;
    private final WALIndexProvider walIndexProvider;
    private final AtomicLong lastEndOfLastRow = new AtomicLong(-1);

    private final RowIOProvider ioProvider;
    private RowIO io;

    public BinaryWALTx(File baseDir,
        String prefix,
        RowIOProvider ioProvider,
        RowMarshaller<byte[]> rowMarshaller,
        WALIndexProvider walIndexProvider) throws Exception {
        this.dir = new File(baseDir, AmzaVersionConstants.LATEST_VERSION);
        this.name = prefix + SUFFIX;
        this.ioProvider = ioProvider;
        this.rowMarshaller = rowMarshaller;
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
    public WALIndex load(RegionName regionName) throws Exception {
        compactionLock.acquire(NUM_PERMITS);
        try {
            final WALIndex walIndex = walIndexProvider.createIndex(regionName);
            if (walIndex.isEmpty()) {
                LOG.info(
                    "Rebuilding " + walIndex.getClass().getSimpleName()
                        + " for " + regionName.getRegionName() + "-" + regionName.getRingName() + "...");
                final MutableLong rebuilt = new MutableLong();
                io.scan(0, new RowStream() {
                    @Override
                    public boolean row(final long rowPointer, long rowTxId, byte rowType, byte[] row) throws Exception {
                        if (rowType > 0) {
                            RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                            WALKey key = walr.getKey();
                            WALValue value = walr.getValue();
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
                    }
                });
                LOG.info("Rebuilt (" + rebuilt.longValue() + ")" + walIndex.getClass().getSimpleName()
                    + " for " + regionName.getRegionName() + "-" + regionName.getRingName() + ".");
                walIndex.commit();
            } else {
                LOG.info("Checking " + walIndex.getClass().getSimpleName()
                    + " for " + regionName.getRegionName() + "-" + regionName.getRingName() + ".");
                final MutableLong repair = new MutableLong();
                io.reverseScan(new RowStream() {
                    long commitedUpToTxId = Long.MIN_VALUE;

                    @Override
                    public boolean row(long rowFP, long rowTxId, byte rowType, byte[] row) throws Exception {
                        if (rowType > 0) {
                            RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                            WALKey key = walr.getKey();
                            WALValue value = walr.getValue();
                            walIndex.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                                key, new WALPointer(rowFP, value.getTimestampId(), value.getTombstoned()))));
                            repair.add(1);
                        }
                        if (rowType == WALWriter.SYSTEM_VERSION_1 && commitedUpToTxId == Long.MIN_VALUE) {
                            long[] key_CommitedUpToTxId = UIO.bytesLongs(row);
                            if (key_CommitedUpToTxId[0] == WALWriter.COMMIT_MARKER) {
                                commitedUpToTxId = key_CommitedUpToTxId[1];
                            }
                            return true;
                        } else {
                            return rowTxId >= commitedUpToTxId;
                        }
                    }
                });
                LOG.info("Checked (" + repair.longValue() + ")" + walIndex.getClass().getSimpleName()
                    + " for " + regionName.getRegionName() + "-" + regionName.getRingName() + ".");
                walIndex.commit();
            }
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
                FileUtils.deleteDirectory(dir);
                return true;
            }
            return false;
        } finally {
            compactionLock.release(NUM_PERMITS);
        }
    }

    @Override
    public Optional<Compacted> compact(final RegionName regionName,
        final long removeTombstonedOlderThanTimestampId,
        final long ttlTimestampId,
        final WALIndex rowIndex) throws Exception {

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

        return Optional.<Compacted>of(new Compacted() {

            @Override
            public CommittedCompacted commit() throws Exception {

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
                    LOG.set(ValueType.COUNT, metricPrefix + "ttl", ttlCount.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "duration", System.currentTimeMillis() - start);
                    LOG.set(ValueType.COUNT, metricPrefix + "catchupKeeps", catchupKeys.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "catchupRemoves", catchupRemoves.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "catchupTombstones", catchupTombstones.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "catchupTtl", catchupTTL.get());
                    LOG.set(ValueType.COUNT, metricPrefix + "catchupDuration", System.currentTimeMillis() - startCatchup);

                    LOG.info("Compacted region " + dir.getAbsolutePath() + "/" + name
                        + " was:" + sizeBeforeCompaction + "bytes "
                        + " isNow:" + sizeAfterCompaction + "bytes.");
                    if (compactionRowIndex != null) {
                        compactionRowIndex.commit();
                    }

                    long endOfLastRow = io.getEndOfLastRow();
                    lastEndOfLastRow.set(endOfLastRow);
                    return new CommittedCompacted(sizeAfterCompaction, rowIndex);
                } finally {
                    compactionLock.release(NUM_PERMITS);
                }
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
        final List<Byte> rawRowTypes = new ArrayList<>();
        final List<byte[]> rawRows = new ArrayList<>();
        final AtomicLong batchSizeInBytes = new AtomicLong();

        io.scan(startAtRow, new RowStream() {
            @Override
            public boolean row(final long rowFP, long rowTxId, byte rowType, final byte[] row) throws Exception {
                if (rowFP >= endOfLastRow) {
                    return false;
                }
                if (rowType < 0) { // TODO expose to caller which rowtypes they want to preserve. For now we discard all system rowtypes and keep all others.
                    return true;
                }

                RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                WALKey key = walr.getKey();
                WALValue value = walr.getValue();

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
                long batchingSize = 1024 * 1024 * 10; // TODO expose to config
                if (batchSizeInBytes.get() > batchingSize) {
                    flush(compactionWALIndex, compactionIO, rowTxIds, rawRowTypes, rawRows, rowKeys, rowValues, batchSizeInBytes);
                }
                return true;
            }

        });
        if (!rawRows.isEmpty()) {
            flush(compactionWALIndex, compactionIO, rowTxIds, rawRowTypes, rawRows, rowKeys, rowValues, batchSizeInBytes);
        }
    }

    private void flush(CompactionWALIndex compactionWALIndex,
        RowIO compactionIO,
        List<Long> rowTxIds,
        List<Byte> rawRowTypes,
        List<byte[]> rawRows,
        List<WALKey> rowKeys,
        List<WALValue> rowValues,
        AtomicLong batchSizeInBytes) throws Exception {

        List<byte[]> rowPointers = compactionIO.write(rowTxIds, rawRowTypes, rawRows);
        Collection<Map.Entry<WALKey, WALPointer>> entries = new ArrayList<>(rowKeys.size());
        for (int i = 0; i < rowKeys.size(); i++) {
            WALValue rowValue = rowValues.get(i);
            entries.add(new AbstractMap.SimpleEntry<>(rowKeys.get(i),
                new WALPointer(UIO.bytesLong(rowPointers.get(i)), rowValue.getTimestampId(), rowValue.getTombstoned())));
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
