package com.jivesoftware.os.amza.storage;

import com.google.common.base.Optional;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.jivesoftware.os.amza.shared.Commitable;
import com.jivesoftware.os.amza.shared.Highwaters;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowType;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.WALHighwater;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class NonIndexWAL implements WALStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RegionName regionName;
    private final OrderIdProvider orderIdProvider;
    private final PrimaryRowMarshaller<byte[]> rowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final WALTx wal;
    private final Object oneTransactionAtATimeLock = new Object();
    private final AtomicLong updateCount = new AtomicLong();

    public NonIndexWAL(RegionName regionName,
        OrderIdProvider orderIdProvider,
        PrimaryRowMarshaller<byte[]> rowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        WALTx rowsTx) {
        this.regionName = regionName;
        this.orderIdProvider = orderIdProvider;
        this.rowMarshaller = rowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.wal = rowsTx;
    }

    public RegionName getRegionName() {
        return regionName;
    }

    @Override
    public long compactTombstone(long removeTombstonedOlderThanTimestampId, long ttlTimestampId) throws Exception {
        if (updateCount.get() > 0) {
            Optional<WALTx.Compacted> compact = wal.compact(removeTombstonedOlderThanTimestampId, ttlTimestampId, null);
            if (compact.isPresent()) {
                WALTx.CommittedCompacted compacted = compact.get().commit();
                updateCount.set(0);
                return compacted.sizeAfterCompaction;
            }
        }
        return -1;
    }

    @Override
    public void load() throws Exception {
    }

    @Override
    public void flush(boolean fsync) throws Exception {
        wal.flush(fsync);
    }

    @Override
    public boolean delete(boolean ifEmpty) throws Exception {
        return wal.delete(ifEmpty);
    }

    @Override
    public RowsChanged update(final boolean useUpdateTxId,
        WALReplicator walReplicator,
        WALStorageUpdateMode updateMode,
        Commitable<WALValue> updates) throws Exception {

        final AtomicLong oldestApplied = new AtomicLong(Long.MAX_VALUE);
        final Table<Long, WALKey, WALValue> apply = TreeBasedTable.create();

        updates.commitable(null, (long transactionId, WALKey key, WALValue update) -> {
            apply.put(transactionId, key, update);
            if (oldestApplied.get() > update.getTimestampId()) {
                oldestApplied.set(update.getTimestampId());
            }
            return true;
        });

        if (apply.isEmpty()) {
            return new RowsChanged(regionName, oldestApplied.get(), apply, new TreeMap<>(), new TreeMap<>());
        } else {
            wal.write((WALWriter rowWriter) -> {
                List<Long> transactionIds = useUpdateTxId ? new ArrayList<>() : null;
                List<byte[]> rawRows = new ArrayList<>();
                for (Table.Cell<Long, WALKey, WALValue> cell : apply.cellSet()) {
                    if (useUpdateTxId) {
                        transactionIds.add(cell.getRowKey());
                    }
                    WALKey key = cell.getColumnKey();
                    WALValue value = cell.getValue();
                    rawRows.add(rowMarshaller.toRow(key, value));
                }
                synchronized (oneTransactionAtATimeLock) {
                    if (!useUpdateTxId) {
                        long transactionId = orderIdProvider.nextId();
                        transactionIds = Collections.nCopies(rawRows.size(), transactionId);
                    }
                    rowWriter.writePrimary(transactionIds, rawRows);
                }
                return null;
            });
            updateCount.addAndGet(apply.size());
            return new RowsChanged(regionName, oldestApplied.get(), apply, new TreeMap<>(), new TreeMap<>());
        }

    }

    @Override
    public void rowScan(final Scan<WALValue> scan) throws Exception {
        wal.read((WALReader reader) -> {
            reader.scan(0, false,
                (long rowFP, long rowTxId, RowType rowType, byte[] rawWRow) -> {
                    if (rowType == RowType.primary) {
                        WALRow row = rowMarshaller.fromRow(rawWRow);
                        return scan.row(rowTxId, row.key, row.value);
                    }
                    return true;
                });
            return null;
        });
    }

    @Override
    public void rangeScan(final WALKey from, final WALKey to, final Scan<WALValue> scan) throws Exception {
        wal.read((WALReader reader) -> {
            reader.scan(0, false,
                (long rowPointer, long rowTxId, RowType rowType, byte[] rawRow) -> {
                    if (rowType == RowType.primary) {
                        WALRow row = rowMarshaller.fromRow(rawRow);
                        if (row.key.compareTo(to) < 0) {
                            if (from.compareTo(row.key) <= 0) {
                                scan.row(rowTxId, row.key, row.value);
                            }
                            return true;
                        } else {
                            return false;
                        }
                    }
                    return true;
                });
            return null;
        });

    }

    @Override
    public WALValue get(WALKey key) throws Exception {
        throw new UnsupportedOperationException("NonIndexWAL doesn't support gets.");
    }

    @Override
    public WALValue[] get(WALKey[] keys) throws Exception {
        throw new UnsupportedOperationException("NonIndexWAL doesn't support gets.");
    }

    @Override
    public WALPointer[] getPointers(WALKey[] consumableKeys, List<WALValue> values) throws Exception {
        throw new UnsupportedOperationException("NonIndexWAL doesn't support getPointers.");
    }

    @Override
    public boolean containsKey(WALKey key) throws Exception {
        throw new UnsupportedOperationException("NonIndexWAL doesn't support containsKey.");
    }

    @Override
    public List<Boolean> containsKey(List<WALKey> keys) throws Exception {
        throw new UnsupportedOperationException("NonIndexWAL doesn't support containsKey.");
    }

    @Override
    public long count() throws Exception {
        throw new UnsupportedOperationException("NonIndexWAL doesn't support count.");
    }

    @Override
    public long highestTxId() {
        throw new UnsupportedOperationException("NonIndexWAL doesn't support highestTxId.");
    }

    @Override
    public boolean takeRowUpdatesSince(final long sinceTransactionId, final RowStream rowStream) throws Exception {
        return wal.readFromTransactionId(sinceTransactionId, (long offset, WALReader rowReader) -> rowReader.scan(offset, false,
            (long rowPointer, long rowTxId, RowType rowType, byte[] row) -> {
                if (rowType != RowType.system && rowTxId > sinceTransactionId) {
                    return rowStream.row(rowPointer, rowTxId, rowType, row);
                }
                return true;
            }));
    }

    @Override
    public boolean takeFromTransactionId(final long sinceTransactionId, Highwaters highwaters, final Scan<WALValue> scan) throws Exception {
        return wal.readFromTransactionId(sinceTransactionId, (long offset, WALReader rowReader) -> rowReader.scan(offset, false,
            (long rowPointer, long rowTxId, RowType rowType, byte[] row) -> {
                if (rowType == RowType.highwater && highwaters != null) {
                    WALHighwater highwater = highwaterRowMarshaller.fromBytes(row);
                    highwaters.highwater(highwater);
                } else if (rowType == RowType.primary && rowTxId > sinceTransactionId) {
                    WALRow walRow = rowMarshaller.fromRow(row);
                    return scan.row(rowTxId, walRow.key, walRow.value);
                }
                return true;
            }));
    }

    @Override
    public void updatedStorageDescriptor(WALStorageDescriptor walStorageDescriptor) throws Exception {
    }
}
