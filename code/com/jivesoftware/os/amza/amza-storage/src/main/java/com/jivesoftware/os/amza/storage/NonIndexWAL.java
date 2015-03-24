package com.jivesoftware.os.amza.storage;

import com.google.common.collect.ArrayListMultimap;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class NonIndexWAL implements WALStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RegionName regionName;
    private final OrderIdProvider orderIdProvider;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final WALTx rowsTx;

    public NonIndexWAL(RegionName regionName,
        OrderIdProvider orderIdProvider,
        RowMarshaller<byte[]> rowMarshaller,
        WALTx rowsTx) {
        this.regionName = regionName;
        this.orderIdProvider = orderIdProvider;
        this.rowMarshaller = rowMarshaller;
        this.rowsTx = rowsTx;
    }

    public RegionName getRegionName() {
        return regionName;
    }

    @Override
    public void compactTombstone(long removeTombstonedOlderThanTimestampId) throws Exception {
        rowsTx.compact(regionName, removeTombstonedOlderThanTimestampId, null);
    }

    @Override
    public void load() throws Exception {

    }

    @Override
    public RowsChanged update(WALScanable updates) throws Exception {
        final AtomicLong oldestApplied = new AtomicLong(Long.MAX_VALUE);
        final NavigableMap<WALKey, WALValue> apply = new TreeMap<>();

        updates.rowScan(new WALScan() {
            @Override
            public boolean row(long transactionId, WALKey key, WALValue update) throws Exception {
                apply.put(key, update);
                if (oldestApplied.get() > update.getTimestampId()) {
                    oldestApplied.set(update.getTimestampId());
                }
                return true;
            }
        });

        if (apply.isEmpty()) {
            return new RowsChanged(regionName, oldestApplied.get(), apply, new TreeMap<WALKey, WALValue>(), ArrayListMultimap.<WALKey, WALValue>create());
        } else {
            final long transactionId = (orderIdProvider == null) ? 0 : orderIdProvider.nextId();
            rowsTx.write(new WALTx.WALWrite<Void>() {
                @Override
                public Void write(WALWriter rowWriter) throws Exception {

                    List<byte[]> rawRows = new ArrayList<>();
                    for (Map.Entry<WALKey, WALValue> e : apply.entrySet()) {
                        WALKey key = e.getKey();
                        WALValue value = e.getValue();
                        rawRows.add(rowMarshaller.toRow(transactionId, key, value));
                    }
                    rowWriter.write(Collections.nCopies(rawRows.size(), (byte) 0), rawRows, true);
                    return null;
                }
            });
            return new RowsChanged(regionName, oldestApplied.get(), apply, new TreeMap<WALKey, WALValue>(), ArrayListMultimap.<WALKey, WALValue>create());
        }

    }

    @Override
    public void rowScan(final WALScan walScan) throws Exception {
        rowsTx.read(new WALTx.WALRead<Void>() {

            @Override
            public Void read(WALReader reader) throws Exception {
                reader.scan(0, new WALReader.Stream() {

                    @Override
                    public boolean row(long rowPointer, byte rowType, byte[] rawWRow) throws Exception {
                        RowMarshaller.WALRow row = rowMarshaller.fromRow(rawWRow);
                        return walScan.row(row.getTransactionId(), row.getKey(), row.getValue());
                    }
                });
                return null;
            }
        });
    }

    @Override
    public void rangeScan(final WALKey from, final WALKey to, final WALScan walScan) throws Exception {
        rowsTx.read(new WALTx.WALRead<Void>() {

            @Override
            public Void read(WALReader reader) throws Exception {
                reader.scan(0, new WALReader.Stream() {

                    @Override
                    public boolean row(long rowPointer, byte rowType, byte[] rawWRow) throws Exception {
                        RowMarshaller.WALRow row = rowMarshaller.fromRow(rawWRow);
                        if (row.getKey().compareTo(to) < 0) {
                            if (from.compareTo(row.getKey()) <= 0) {
                                walScan.row(row.getTransactionId(), row.getKey(), row.getValue());
                            }
                            return true;
                        } else {
                            return false;
                        }
                    }
                });
                return null;
            }
        });

    }

    @Override
    public WALValue get(WALKey key) throws Exception {
        return get(Collections.singletonList(key)).get(0);
    }

    @Override
    public List<WALValue> get(List<WALKey> keys) throws Exception {
        throw new UnsupportedOperationException("NonIndexWAL doesn't support gets.");
    }

    @Override
    public boolean containsKey(WALKey key) throws Exception {
        return containsKey(Collections.singletonList(key)).get(0);
    }

    @Override
    public List<Boolean> containsKey(List<WALKey> keys) throws Exception {
        throw new UnsupportedOperationException("NonIndexWAL doesn't support gets.");
    }

    @Override
    public void takeRowUpdatesSince(final long sinceTransactionId, final WALScan walScan) throws Exception {
        final WALScan filteringRowStream = new WALScan() {
            @Override
            public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
                if (transactionId > sinceTransactionId) {
                    if (!walScan.row(transactionId, key, value)) {
                        return false;
                    }
                }
                return true;
            }
        };

        rowsTx.read(new WALTx.WALRead<Void>() {

            @Override
            public Void read(WALReader rowReader) throws Exception {
                rowReader.reverseScan(new WALReader.Stream() {
                    @Override
                    public boolean row(long rowPointer, byte rowType, byte[] row) throws Exception {
                        RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                        return filteringRowStream.row(walr.getTransactionId(), walr.getKey(), walr.getValue());
                    }
                });
                return null;
            }
        });

    }

}
