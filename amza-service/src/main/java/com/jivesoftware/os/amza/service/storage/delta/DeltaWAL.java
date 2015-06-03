package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.region.VersionedRegionName;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALReader;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.shared.wal.WALWriter;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.storage.WALRow;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class DeltaWAL implements WALRowHydrator, Comparable<DeltaWAL> {

    private final long id;
    private final OrderIdProvider orderIdProvider;
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final WALTx wal;
    private final AtomicLong updateCount = new AtomicLong();
    private final Object oneTxAtATimeLock = new Object();

    public DeltaWAL(long id,
        OrderIdProvider orderIdProvider,
        PrimaryRowMarshaller<byte[]> primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        WALTx wal) {
        this.id = id;
        this.orderIdProvider = orderIdProvider;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.wal = wal;
    }

    public void load(final RowStream rowStream) throws Exception {
        wal.read((WALReader reader) -> {
            reader.scan(0, true, rowStream);
            return null;
        });
    }

    public void flush(boolean fsync) throws Exception {
        wal.flush(fsync);
    }

    WALKey regionPrefixedKey(VersionedRegionName versionedRegionName, WALKey key) throws IOException {
        byte[] regionNameBytes = versionedRegionName.toBytes();
        ByteBuffer bb = ByteBuffer.allocate(2 + regionNameBytes.length + 4 + key.getKey().length);
        bb.putShort((short) regionNameBytes.length);
        bb.put(regionNameBytes);
        bb.putInt(key.getKey().length);
        bb.put(key.getKey());
        return new WALKey(bb.array());
    }

    WALValue appendHighwaterHints(WALValue value, WALHighwater hints) throws Exception {
        HeapFiler filer = new HeapFiler();
        UIO.writeByteArray(filer, value.getValue(), "value");
        if (hints != null) {
            UIO.writeBoolean(filer, true, "hasHighwaterHints");
            UIO.writeByteArray(filer, highwaterRowMarshaller.toBytes(hints), "highwaterHints");
        } else {
            UIO.writeBoolean(filer, false, "hasHighwaterHints");
        }
        return new WALValue(filer.getBytes(), value.getTimestampId(), value.getTombstoned());
    }

    public DeltaWALApplied update(final VersionedRegionName versionedRegionName,
        final Table<Long, WALKey, WALValue> apply,
        WALHighwater highwaterHint) throws Exception {
        final Map<WALKey, Long> keyToRowPointer = new HashMap<>();

        final MutableLong txId = new MutableLong();
        wal.write((WALWriter rowWriter) -> {
            List<WALKey> keys = new ArrayList<>();
            List<byte[]> rawRows = new ArrayList<>();
            Set<Table.Cell<Long, WALKey, WALValue>> applies = apply.cellSet();
            int count = applies.size();
            for (Table.Cell<Long, WALKey, WALValue> cell : applies) {
                count--;
                WALKey key = cell.getColumnKey();
                WALValue value = cell.getValue();
                keys.add(key);
                key = regionPrefixedKey(versionedRegionName, key);
                value = appendHighwaterHints(value, count == 0 ? highwaterHint : null);
                rawRows.add(primaryRowMarshaller.toRow(key, value));

            }
            long transactionId;
            long[] rowPointers;
            synchronized (oneTxAtATimeLock) {
                transactionId = (orderIdProvider == null) ? 0 : orderIdProvider.nextId();
                rowPointers = rowWriter.writePrimary(Collections.nCopies(rawRows.size(), transactionId), rawRows);
            }
            txId.setValue(transactionId);
            for (int i = 0; i < rowPointers.length; i++) {
                keyToRowPointer.put(keys.get(i), rowPointers[i]);
            }
            return null;
        });
        updateCount.addAndGet(apply.size());
        return new DeltaWALApplied(keyToRowPointer, txId.longValue());

    }

    boolean takeRows(final NavigableMap<Long, List<Long>> tailMap, RowStream rowStream) throws Exception {
        return wal.read((WALReader reader) -> {
            // reverse everything so highest FP is first, helps minimize mmap extensions
            for (Long key : tailMap.descendingKeySet()) {
                List<Long> rowFPs = Lists.reverse(tailMap.get(key));
                for (long fp : rowFPs) {
                    byte[] rawRow = reader.read(fp);
                    WALRow row = primaryRowMarshaller.fromRow(rawRow);
                    ByteBuffer bb = ByteBuffer.wrap(row.key.getKey());
                    byte[] regionNameBytes = new byte[bb.getShort()];
                    bb.get(regionNameBytes);
                    byte[] keyBytes = new byte[bb.getInt()];
                    bb.get(keyBytes);

                    WALValue value = row.value;
                    HeapFiler filer = new HeapFiler(value.getValue());
                    value = new WALValue(UIO.readByteArray(filer, "value"), value.getTimestampId(), value.getTombstoned());
                    if (!rowStream.row(fp, key, RowType.primary, primaryRowMarshaller.toRow(new WALKey(keyBytes), value))) {
                        return false;
                    }
                    if (UIO.readBoolean(filer, "hasHighwaterHints")) {
                        if (!rowStream.row(-1, -1, RowType.highwater, UIO.readByteArray(filer, "highwaterHints"))) {
                            return false;
                        }
                    }
                }
            }
            return true;
        });

    }

    @Override
    public WALRow hydrate(final long fp) throws Exception {
        try {
            byte[] row = wal.read((WALReader rowReader) -> rowReader.read(fp));
            final WALRow walRow = primaryRowMarshaller.fromRow(row);
            WALValue value = walRow.value;
            HeapFiler filer = new HeapFiler(value.getValue());
            value = new WALValue(UIO.readByteArray(filer, "value"), value.getTimestampId(), value.getTombstoned());
            return new WALRow(walRow.key, value);
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate fp:" + fp + " length:" + wal.length(), x);
        }
    }

    public KeyValueHighwater hydrateKeyValueHighwater(long fp) throws Exception {
        try {
            byte[] row = wal.read((WALReader rowReader) -> rowReader.read(fp));
            WALRow walRow = primaryRowMarshaller.fromRow(row);
            HeapFiler filer = new HeapFiler(walRow.value.getValue());
            WALValue value = new WALValue(UIO.readByteArray(filer, "value"), walRow.value.getTimestampId(), walRow.value.getTombstoned());
            WALHighwater highwater = null;
            if (UIO.readBoolean(filer, "hasHighwaterHint")) {
                highwater = highwaterRowMarshaller.fromBytes(UIO.readByteArray(filer, "highwaters"));
            }
            return new KeyValueHighwater(walRow.key, value, highwater);
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate fp:" + fp + " length:" + wal.length(), x);
        }
    }

    public static class KeyValueHighwater {

        public final WALKey key;
        public final WALValue value;
        public final WALHighwater highwater;

        public KeyValueHighwater(WALKey key, WALValue value, WALHighwater highwater) {
            this.key = key;
            this.value = value;
            this.highwater = highwater;
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
