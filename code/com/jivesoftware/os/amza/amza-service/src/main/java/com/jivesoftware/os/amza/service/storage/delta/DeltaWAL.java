package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.WALRow;
import com.jivesoftware.os.amza.storage.binary.BinaryRowWriter;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class DeltaWAL implements WALRowHydrator, Comparable<DeltaWAL> {

    private final long id;
    private final OrderIdProvider orderIdProvider;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final WALTx wal;
    private final AtomicLong updateCount = new AtomicLong();
    private final Object oneTxAtATimeLock = new Object();

    public DeltaWAL(long id,
        OrderIdProvider orderIdProvider,
        RowMarshaller<byte[]> rowMarshaller,
        WALTx wal) {
        this.id = id;
        this.orderIdProvider = orderIdProvider;
        this.rowMarshaller = rowMarshaller;
        this.wal = wal;
    }

    public void load(final RowStream rowStream) throws Exception {
        wal.read(new WALTx.WALRead<Void>() {

            @Override
            public Void read(WALReader reader) throws Exception {
                reader.scan(0, true, rowStream);
                return null;
            }
        });
    }

    public void flush(boolean fsync) throws Exception {
        wal.flush(fsync);
    }

    WALKey regionPrefixedKey(RegionName regionName, WALKey key) throws IOException {
        byte[] regionNameBytes = regionName.toBytes();
        ByteBuffer bb = ByteBuffer.allocate(2 + regionNameBytes.length + 4 + key.getKey().length);
        bb.putShort((short) regionNameBytes.length);
        bb.put(regionNameBytes);
        bb.putInt(key.getKey().length);
        bb.put(key.getKey());
        return new WALKey(bb.array());
    }

    public DeltaWALApplied update(final RegionName regionName, final Table<Long, WALKey, WALValue> apply) throws Exception {
        final Map<WALKey, Long> keyToRowPointer = new HashMap<>();

        final MutableLong txId = new MutableLong();
        wal.write(new WALTx.WALWrite<Void>() {
            @Override
            public Void write(WALWriter rowWriter) throws Exception {
                List<WALKey> keys = new ArrayList<>();
                List<byte[]> rawRows = new ArrayList<>();
                for (Table.Cell<Long, WALKey, WALValue> cell : apply.cellSet()) {
                    WALKey key = cell.getColumnKey();
                    WALValue value = cell.getValue();
                    keys.add(key);
                    key = regionPrefixedKey(regionName, key);
                    rawRows.add(rowMarshaller.toRow(key, value));
                }
                long transactionId;
                long[] rowPointers;
                synchronized (oneTxAtATimeLock) {
                    transactionId = (orderIdProvider == null) ? 0 : orderIdProvider.nextId();
                    rowPointers = rowWriter.write(Collections.nCopies(rawRows.size(), transactionId),
                        Collections.nCopies(rawRows.size(), WALWriter.VERSION_1),
                        rawRows);
                }
                txId.setValue(transactionId);
                for (int i = 0; i < rowPointers.length; i++) {
                    keyToRowPointer.put(keys.get(i), rowPointers[i]);
                }
                return null;
            }
        });
        updateCount.addAndGet(apply.size());
        return new DeltaWALApplied(keyToRowPointer, txId.longValue());

    }

    boolean takeRows(final NavigableMap<Long, List<Long>> tailMap, final RowStream rowStream) throws Exception {
        return wal.read(new WALTx.WALRead<Boolean>() {

            @Override
            public Boolean read(WALReader reader) throws Exception {
                // reverse everything so highest FP is first, helps minimize mmap extensions
                for (Long key : tailMap.descendingKeySet()) {
                    List<Long> rowFPs = Lists.reverse(tailMap.get(key));
                    for (long fp : rowFPs) {
                        byte[] rawRow = reader.read(fp);
                        WALRow row = rowMarshaller.fromRow(rawRow);
                        ByteBuffer bb = ByteBuffer.wrap(row.getKey().getKey());
                        byte[] regionNameBytes = new byte[bb.getShort()];
                        bb.get(regionNameBytes);
                        byte[] keyBytes = new byte[bb.getInt()];
                        bb.get(keyBytes);

                        if (!rowStream.row(fp, key, BinaryRowWriter.VERSION_1,
                            rowMarshaller.toRow(new WALKey(keyBytes), row.getValue()))) { // TODO Ah were to get rowType
                            return false;
                        }
                    }
                }
                return true;
            }
        });

    }

    @Override
    public WALRow hydrate(final long fp) throws Exception {
        try {
            byte[] row = wal.read(new WALTx.WALRead<byte[]>() {
                @Override
                public byte[] read(WALReader rowReader) throws Exception {
                    return rowReader.read(fp);
                }
            });
            return rowMarshaller.fromRow(row);
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate " + fp, x);
        }
    }

    void destroy() throws Exception {
        synchronized (oneTxAtATimeLock) {
            wal.delete(false);
        }
    }

    @Override
    public int compareTo(DeltaWAL o) {
        return Long.compare(id, o.id);
    }

    public static class DeltaWALApplied {

        public final Map<WALKey, Long> keyToRowPointer;
        public final long txId;

        public DeltaWALApplied(Map<WALKey, Long> keyToRowPointer, long txId) {
            this.keyToRowPointer = keyToRowPointer;
            this.txId = txId;
        }
    }
}
