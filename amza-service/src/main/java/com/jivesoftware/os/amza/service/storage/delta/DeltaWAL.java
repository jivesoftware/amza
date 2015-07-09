package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.delta.DeltaValueCache.DeltaRow;
import com.jivesoftware.os.amza.shared.StripedTLongObjectMap;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.wal.FpKeyValueHighwaterStream;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALReader;
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

    // TODO IOC shift to using callback
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
                keyValueHighwaters[index] = new KeyValueHighwater(key.getKey(), value.getValue(), value.getTimestampId(), value.getTombstoned(), highwater);
                key = partitionPrefixedKey(versionedPartitionName, key);
                value = appendHighwaterHints(value, highwater);
                rawRows.add(primaryRowMarshaller.toRow(key.getKey(), value.getValue(), value.getTimestampId(), value.getTombstoned()));
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
                        if (!rowStream.row(fp, txId, RowType.primary,
                            primaryRowMarshaller.toRow(deltaRow.key, deltaRow.value, deltaRow.valueTimestamp, deltaRow.valueTombstone))) {
                            return false;
                        }
                        if (deltaRow.highwater != null && !rowStream.row(-1, -1, RowType.highwater, highwaterRowMarshaller.toBytes(deltaRow.highwater))) {
                            return false;
                        }
                    } else {
                        byte[] rawRow = reader.read(fp);
                        primaryRowMarshaller.fromRow(rawRow, (key, value, valueTimestamp, valueTombstoned) -> {
                            ByteBuffer bb = ByteBuffer.wrap(key);
                            byte[] partitionNameBytes = new byte[bb.getShort()];
                            bb.get(partitionNameBytes);
                            byte[] keyBytes = new byte[bb.getInt()];
                            bb.get(keyBytes);

                            HeapFiler filer = new HeapFiler(value);
                            if (!rowStream.row(fp, txId, RowType.primary, primaryRowMarshaller.toRow(keyBytes,
                                UIO.readByteArray(filer, "value"), valueTimestamp, valueTombstoned))) {
                                return false;
                            }
                            if (UIO.readBoolean(filer, "hasHighwaterHints")) {
                                if (!rowStream.row(-1, -1, RowType.highwater, UIO.readByteArray(filer, "highwaterHints"))) {
                                    return false;
                                }
                            }
                            return true;
                        });
                    }
                }
            }
            return true;
        });

    }

    @Override
    public boolean hydrate(long fp, FpKeyValueHighwaterStream stream) throws Exception {
        try {
            byte[] row = wal.read((rowReader) -> rowReader.read(fp));
            return primaryRowMarshaller.fromRow(row, (key, value, valueTimestamp, valueTombstoned) -> {
                HeapFiler filer = new HeapFiler(value);
                return stream.stream(fp, key, UIO.readByteArray(filer, "value"), valueTimestamp, valueTombstoned, null);
            });
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate fp:" + fp + " length:" + wal.length(), x);
        }
    }

    public boolean hydrateKeyValueHighwater(long fp, FpKeyValueHighwaterStream stream) throws Exception {
        try {
            byte[] row = wal.read((WALReader rowReader) -> rowReader.read(fp));
            return primaryRowMarshaller.fromRow(row, (key, value, valueTimestamp, valueTombstoned) -> {
                return hydrateKeyValueHighwater(fp, key, value, valueTimestamp, valueTombstoned, stream);
            });
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate fp:" + fp + " length:" + wal.length(), x);
        }
    }

    public boolean hydrateKeyValueHighwater(long fp, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstone,
        FpKeyValueHighwaterStream stream) throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(key);
        byte[] partitionNameBytes = new byte[bb.getShort()];
        bb.get(partitionNameBytes);
        final byte[] keyBytes = new byte[bb.getInt()];
        bb.get(keyBytes);

        HeapFiler filer = new HeapFiler(value);
        byte[] hydrateValue = UIO.readByteArray(filer, "value");
        WALHighwater highwater = null;
        if (UIO.readBoolean(filer, "hasHighwaterHint")) {
            highwater = highwaterRowMarshaller.fromBytes(UIO.readByteArray(filer, "highwaters"));
        }
        return stream.stream(fp, keyBytes, hydrateValue, valueTimestamp, valueTombstone, highwater);
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

        public final byte[] key;
        public final byte[] value;
        public final long valueTimestamp;
        public final boolean valueTombstone;
        public final WALHighwater highwater;

        public KeyValueHighwater(byte[] key, byte[] value, long valueTimestamp, boolean valueTombstone, WALHighwater highwater) {
            this.key = key;
            this.value = value;
            this.valueTimestamp = valueTimestamp;
            this.valueTombstone = valueTombstone;
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
