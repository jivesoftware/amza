package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.delta.DeltaValueCache.DeltaRow;
import com.jivesoftware.os.amza.shared.StripedTLongObjectMap;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALReader;
import com.jivesoftware.os.amza.shared.wal.WALRow;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.shared.wal.WALWriter;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
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

    WALKey partitionPrefixedKey(VersionedPartitionName versionedPartitionName, WALKey key) throws IOException {
        byte[] partitionNameBytes = versionedPartitionName.toBytes();
        ByteBuffer bb = ByteBuffer.allocate(2 + partitionNameBytes.length + 4 + key.getKey().length);
        bb.putShort((short) partitionNameBytes.length);
        bb.put(partitionNameBytes);
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

    public DeltaWALApplied update(final VersionedPartitionName versionedPartitionName,
        Map<WALKey, WALValue> apply,
        WALHighwater highwaterHint) throws Exception {

        MutableLong txId = new MutableLong();
        int numApplies = apply.size();
        KeyValueHighwater[] keyValueHighwaters = new KeyValueHighwater[numApplies];
        long[] fps = wal.write((WALWriter rowWriter) -> {
            int index = 0;
            List<byte[]> rawRows = new ArrayList<>();
            for (Map.Entry<WALKey, WALValue> entry : apply.entrySet()) {
                WALKey key = entry.getKey();
                WALValue value = entry.getValue();
                WALHighwater highwater = (index == numApplies - 1) ? highwaterHint : null;
                keyValueHighwaters[index] = new KeyValueHighwater(key, value, highwater);
                key = partitionPrefixedKey(versionedPartitionName, key);
                value = appendHighwaterHints(value, highwater);
                rawRows.add(primaryRowMarshaller.toRow(key, value));
                index++;
            }
            long transactionId;
            long[] rowPointers;
            synchronized (oneTxAtATimeLock) {
                transactionId = (orderIdProvider == null) ? 0 : orderIdProvider.nextId();
                rowPointers = rowWriter.writePrimary(Collections.nCopies(rawRows.size(), transactionId), rawRows);
            }
            txId.setValue(transactionId);
            return rowPointers;
        });
        updateCount.addAndGet(numApplies);
        return new DeltaWALApplied(txId.longValue(), keyValueHighwaters, fps);

    }

    boolean takeRows(final NavigableMap<Long, long[]> tailMap,
        RowStream rowStream,
        DeltaValueCache deltaValueCache,
        StripedTLongObjectMap<DeltaRow> rowMap) throws Exception {
        return wal.read((WALReader reader) -> {
            for (Long txId : tailMap.keySet()) {
                long[] rowFPs = tailMap.get(txId);
                for (long fp : rowFPs) {
                    DeltaRow deltaRow = deltaValueCache.get(fp, rowMap);
                    if (deltaRow != null) {
                        WALKey key = deltaRow.keyValueHighwater.key;
                        WALValue value = deltaRow.keyValueHighwater.value;
                        WALHighwater highwater = deltaRow.keyValueHighwater.highwater;
                        if (!rowStream.row(fp, txId, RowType.primary, primaryRowMarshaller.toRow(key, value))) {
                            return false;
                        }
                        if (highwater != null && !rowStream.row(-1, -1, RowType.highwater, highwaterRowMarshaller.toBytes(highwater))) {
                            return false;
                        }
                    } else {
                        byte[] rawRow = reader.read(fp);
                        WALRow row = primaryRowMarshaller.fromRow(rawRow);
                        ByteBuffer bb = ByteBuffer.wrap(row.key.getKey());
                        byte[] partitionNameBytes = new byte[bb.getShort()];
                        bb.get(partitionNameBytes);
                        byte[] keyBytes = new byte[bb.getInt()];
                        bb.get(keyBytes);

                        WALValue value = row.value;
                        HeapFiler filer = new HeapFiler(value.getValue());
                        value = new WALValue(UIO.readByteArray(filer, "value"), value.getTimestampId(), value.getTombstoned());
                        if (!rowStream.row(fp, txId, RowType.primary, primaryRowMarshaller.toRow(new WALKey(keyBytes), value))) {
                            return false;
                        }
                        if (UIO.readBoolean(filer, "hasHighwaterHints")) {
                            if (!rowStream.row(-1, -1, RowType.highwater, UIO.readByteArray(filer, "highwaterHints"))) {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        });

    }

    @Override
    public WALRow hydrate(long fp) throws Exception {
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
            return hydrateKeyValueHighwater(walRow);
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate fp:" + fp + " length:" + wal.length(), x);
        }
    }

    public KeyValueHighwater hydrateKeyValueHighwater(WALRow walRow) throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(walRow.key.getKey());
        byte[] partitionNameBytes = new byte[bb.getShort()];
        bb.get(partitionNameBytes);
        final byte[] keyBytes = new byte[bb.getInt()];
        bb.get(keyBytes);

        HeapFiler filer = new HeapFiler(walRow.value.getValue());
        WALValue value = new WALValue(UIO.readByteArray(filer, "value"), walRow.value.getTimestampId(), walRow.value.getTombstoned());
        WALHighwater highwater = null;
        if (UIO.readBoolean(filer, "hasHighwaterHint")) {
            highwater = highwaterRowMarshaller.fromBytes(UIO.readByteArray(filer, "highwaters"));
        }
        return new KeyValueHighwater(new WALKey(keyBytes), value, highwater);
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

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + (int) (this.id ^ (this.id >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DeltaWAL other = (DeltaWAL) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
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

    public static class DeltaWALApplied {

        public final long txId;
        public final KeyValueHighwater[] keyValueHighwaters;
        public final long[] fps;

        public DeltaWALApplied(long txId, KeyValueHighwater[] keyValueHighwaters, long[] fps) {
            this.txId = txId;
            this.keyValueHighwaters = keyValueHighwaters;
            this.fps = fps;
        }
    }
}
