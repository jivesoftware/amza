package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.stream.FpKeyValueHighwaterStream;
import com.jivesoftware.os.amza.shared.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.shared.stream.Fps;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALReader;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.shared.wal.WALWriter;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class DeltaWAL<I extends WALIndex> implements WALRowHydrator, Comparable<DeltaWAL> {

    private final long id;
    private final OrderIdProvider orderIdProvider;
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final WALTx<I> wal;
    private final AtomicLong updateCount = new AtomicLong();
    private final Object oneTxAtATimeLock = new Object();

    public DeltaWAL(long id,
        OrderIdProvider orderIdProvider,
        PrimaryRowMarshaller<byte[]> primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        WALTx<I> wal) {
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

    // TODO IOC shift to using callback
    byte[] appendHighwaterHints(byte[] value, WALHighwater hints) throws Exception {
        HeapFiler filer = new HeapFiler();
        UIO.writeByteArray(filer, value, "value");
        if (hints != null) {
            UIO.writeBoolean(filer, true, "hasHighwaterHints");
            UIO.writeByteArray(filer, highwaterRowMarshaller.toBytes(hints), "highwaterHints");
        } else {
            UIO.writeBoolean(filer, false, "hasHighwaterHints");
        }
        return filer.getBytes();
    }

    public DeltaWALApplied update(final VersionedPartitionName versionedPartitionName,
        Map<WALKey, WALValue> apply,
        WALHighwater highwaterHint) throws Exception {

        MutableLong txId = new MutableLong();
        int numApplies = apply.size();
        KeyValueHighwater[] keyValueHighwaters = new KeyValueHighwater[numApplies];
        long[] fps = new long[numApplies];
        int index = 0;
        for (Map.Entry<WALKey, WALValue> entry : apply.entrySet()) {
            byte[] prefix = entry.getKey().prefix;
            byte[] key = entry.getKey().key;
            WALValue value = entry.getValue();
            WALHighwater highwater = (index == numApplies - 1) ? highwaterHint : null;
            keyValueHighwaters[index] = new KeyValueHighwater(prefix, key, value.getValue(), value.getTimestampId(), value.getTombstoned(), highwater);
            index++;
        }
        wal.write((WALWriter rowWriter) -> {
            long transactionId;
            MutableInt fpIndex = new MutableInt(0);
            synchronized (oneTxAtATimeLock) {
                transactionId = (orderIdProvider == null) ? 0 : orderIdProvider.nextId();
                rowWriter.write(transactionId,
                    RowType.primary,
                    rowStream -> {
                        for (KeyValueHighwater kvh : keyValueHighwaters) {
                            byte[] pk = WALKey.compose(versionedPartitionName.toBytes(), WALKey.compose(kvh.prefix, kvh.key));
                            byte[] value = appendHighwaterHints(kvh.value, kvh.highwater);
                            byte[] row = primaryRowMarshaller.toRow(pk, value, kvh.valueTimestamp, kvh.valueTombstone);
                            if (!rowStream.stream(row)) {
                                return false;
                            }
                        }
                        return true;
                    },
                    indexKeyStream -> {
                        for (KeyValueHighwater kvh : keyValueHighwaters) {
                            if (!indexKeyStream.stream(kvh.prefix, kvh.key, kvh.valueTimestamp, kvh.valueTombstone)) {
                                return false;
                            }
                        }
                        return true;
                    },
                    (rowTxId, prefix, key, valueTimestamp, valueTombstoned, fp) -> {
                        fps[fpIndex.intValue()] = fp;
                        fpIndex.increment();
                        return true;
                    });
            }
            txId.setValue(transactionId);
            return null;
        });
        updateCount.addAndGet(numApplies);
        return new DeltaWALApplied(txId.longValue(), keyValueHighwaters, fps);
    }

    public interface ConsumeTxFps {
        boolean consume(TxFpsStream txFpsStream) throws Exception;
    }

    boolean takeRows(ConsumeTxFps consumeTxFps, RowStream rowStream) throws Exception {
        return wal.read(reader -> primaryRowMarshaller.fromRows(
            txFpRowStream -> consumeTxFps.consume(txFps -> reader.read(
                fpStream -> {
                    for (long fp : txFps.fps) {
                        if (!fpStream.stream(fp)) {
                            return false;
                        }
                    }
                    return true;
                },
                (rowFP, rowTxId, rowType, row) -> txFpRowStream.stream(rowTxId, rowFP, row))),
            (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, row) -> {
                HeapFiler filer = new HeapFiler(value);
                byte[] deltaRow = primaryRowMarshaller.toRow(key, UIO.readByteArray(filer, "value"), valueTimestamp, valueTombstoned);
                if (!rowStream.row(fp, txId, RowType.primary, deltaRow)) {
                    return false;
                }
                if (UIO.readBoolean(filer, "hasHighwaterHints")) {
                    if (!rowStream.row(-1, -1, RowType.highwater, UIO.readByteArray(filer, "highwaterHints"))) {
                        return false;
                    }
                }
                return true;
            }));
    }

    @Override
    public WALValue hydrate(long fp) throws Exception {
        try {
            byte[] row = wal.read(rowReader -> rowReader.read(fp));
            byte[] value = primaryRowMarshaller.valueFromRow(row);
            byte[] deltaValue = UIO.readByteArray(value, 0, "value");
            return new WALValue(
                deltaValue,
                primaryRowMarshaller.timestampFromRow(row),
                primaryRowMarshaller.tombstonedFromRow(row));
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate fp:" + fp + ", WAL length:" + wal.length(), x);
        }
    }

    @Override
    public boolean hydrate(Fps fps, FpKeyValueStream fpKeyValueStream) throws Exception {
        try {
            return primaryRowMarshaller.fromRows(
                (PrimaryRowMarshaller.FpRows) fpRowStream -> wal.read(
                    reader -> reader.read(fps, (rowFP, rowTxId, rowType, row) -> fpRowStream.stream(rowFP, row))),
                (fp, prefix, key, value, valueTimestamp, valueTombstoned) -> {
                    byte[] deltaValue = UIO.readByteArray(value, 0, "value");
                    return fpKeyValueStream.stream(fp, prefix, key, deltaValue, valueTimestamp, valueTombstoned);
                });
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate fps, WAL length:" + wal.length(), x);
        }
    }

    public boolean hydrateKeyValueHighwaters(Fps fps, FpKeyValueHighwaterStream stream) throws Exception {
        return primaryRowMarshaller.fromRows(
            fpRowStream -> wal.read(reader -> reader.read(fps, (rowFP, rowTxId, rowType, row) -> fpRowStream.stream(rowTxId, rowFP, row))),
            (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, row) -> {
                try {
                    HeapFiler filer = new HeapFiler(value);
                    byte[] hydrateValue = UIO.readByteArray(filer, "value");
                    WALHighwater highwater = null;
                    if (UIO.readBoolean(filer, "hasHighwaterHint")) {
                        highwater = highwaterRowMarshaller.fromBytes(UIO.readByteArray(filer, "highwaters"));
                    }
                    return stream.stream(fp, prefix, key, hydrateValue, valueTimestamp, valueTombstoned, highwater);
                } catch (Exception x) {
                    throw new RuntimeException("Failed to hydrate fp:" + fp + " length:" + wal.length(), x);
                }
            });
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

        public final byte[] prefix;
        public final byte[] key;
        public final byte[] value;
        public final long valueTimestamp;
        public final boolean valueTombstone;
        public final WALHighwater highwater;

        public KeyValueHighwater(byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstone, WALHighwater highwater) {
            this.prefix = prefix;
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
