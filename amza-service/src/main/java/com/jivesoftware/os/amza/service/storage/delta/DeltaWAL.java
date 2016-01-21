package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.stream.FpKeyValueHighwaterStream;
import com.jivesoftware.os.amza.api.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.api.stream.Fps;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALTx;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class DeltaWAL implements WALRowHydrator, Comparable<DeltaWAL> {

    private final long id;
    private final long prevId;
    private final OrderIdProvider orderIdProvider;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final WALTx wal;
    private final AtomicLong updateCount = new AtomicLong();
    private final Object oneTxAtATimeLock = new Object();

    public DeltaWAL(long id,
        long prevId,
        OrderIdProvider orderIdProvider,
        PrimaryRowMarshaller primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        WALTx wal) {
        this.id = id;
        this.prevId = prevId;
        this.orderIdProvider = orderIdProvider;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.wal = wal;
    }

    public long getId() {
        return id;
    }

    public long getPrevId() {
        return prevId;
    }

    public void load(final RowStream rowStream) throws Exception {
        wal.tx(io -> {
            io.scan(0, true, rowStream);
            return null;
        });
    }

    public void flush(boolean fsync) throws Exception {
        wal.flush(fsync);
    }

    // TODO IOC shift to using callback
    private byte[] appendHighwaterHints(byte[] value, WALHighwater hints) throws Exception {
        byte[] lengthBuffer = new byte[4];
        if (hints != null) {
            byte[] hintsBytes = highwaterRowMarshaller.toBytes(hints);
            HeapFiler filer = new HeapFiler(4 + (value != null ? value.length : 0) + 1 + 4 + hintsBytes.length);
            UIO.writeByteArray(filer, value, "value", lengthBuffer);
            UIO.write(filer, new byte[]{1}, "hasHighwaterHints");
            UIO.writeByteArray(filer, hintsBytes, "highwaterHints", lengthBuffer);
            return filer.getBytes();
        } else {
            HeapFiler filer = new HeapFiler(4 + (value != null ? value.length : 0) + 1);
            UIO.writeByteArray(filer, value, "value", lengthBuffer);
            UIO.write(filer, new byte[]{0}, "hasHighwaterHints");
            return filer.getBytes();
        }
    }

    private int sizeWithAppendedHighwaterHints(byte[] value, WALHighwater hints) {
        if (hints != null) {
            return 4 + (value != null ? value.length : 0) + 1 + 4 + highwaterRowMarshaller.sizeInBytes(hints);
        } else {
            return 4 + (value != null ? value.length : 0) + 1;
        }
    }

    public DeltaWALApplied update(RowType rowType,
        VersionedPartitionName versionedPartitionName,
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
            keyValueHighwaters[index] = new KeyValueHighwater(rowType, prefix, key,
                value.getValue(), value.getTimestampId(), value.getTombstoned(), value.getVersion(), highwater);
            index++;
        }
        wal.tx(io -> {
            long transactionId;
            MutableInt fpIndex = new MutableInt(0);
            synchronized (oneTxAtATimeLock) {
                transactionId = (orderIdProvider == null) ? 0 : orderIdProvider.nextId();
                int estimatedSizeInBytes = 0;
                for (KeyValueHighwater kvh : keyValueHighwaters) {
                    int pkSizeInBytes = WALKey.sizeOfComposed(versionedPartitionName.sizeInBytes(),
                        WALKey.sizeOfComposed(kvh.prefix != null ? kvh.prefix.length : 0, kvh.key.length));
                    int valueSizeInBytes = sizeWithAppendedHighwaterHints(kvh.value, kvh.highwater);
                    estimatedSizeInBytes += primaryRowMarshaller.maximumSizeInBytes(rowType, pkSizeInBytes, valueSizeInBytes);
                }
                io.write(transactionId,
                    rowType,
                    keyValueHighwaters.length,
                    estimatedSizeInBytes,
                    rowStream -> {
                        for (KeyValueHighwater kvh : keyValueHighwaters) {
                            byte[] pk = WALKey.compose(versionedPartitionName.toBytes(), WALKey.compose(kvh.prefix, kvh.key));
                            byte[] value = appendHighwaterHints(kvh.value, kvh.highwater);
                            byte[] row = primaryRowMarshaller.toRow(rowType, pk, value, kvh.valueTimestamp, kvh.valueTombstone, kvh.valueVersion);
                            if (!rowStream.stream(row)) {
                                return false;
                            }
                        }
                        return true;
                    },
                    indexKeyStream -> {
                        for (KeyValueHighwater kvh : keyValueHighwaters) {
                            if (!indexKeyStream.stream(kvh.prefix, kvh.key, kvh.valueTimestamp, kvh.valueTombstone, kvh.valueVersion)) {
                                return false;
                            }
                        }
                        return true;
                    },
                    (rowTxId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> {
                        fps[fpIndex.intValue()] = fp;
                        fpIndex.increment();
                        return true;
                    },
                    false);
            }
            txId.setValue(transactionId);
            return null;
        });
        updateCount.addAndGet(numApplies);
        return new DeltaWALApplied(txId.longValue(), keyValueHighwaters, fps);
    }

    public void hackTruncation(int numBytes) {
        wal.hackTruncation(numBytes);
    }

    public interface ConsumeTxFps {

        boolean consume(TxFpsStream txFpsStream) throws Exception;
    }

    boolean takeRows(ConsumeTxFps consumeTxFps, RowStream rowStream) throws Exception {
        byte[] intBuffer = new byte[4];
        return wal.tx(io -> primaryRowMarshaller.fromRows(
            txFpRowStream -> consumeTxFps.consume(txFps -> io.read(
                fpStream -> {
                    for (long fp : txFps.fps) {
                        if (!fpStream.stream(fp)) {
                            return false;
                        }
                    }
                    return true;
                },
                (rowFP, rowTxId, rowType, row) -> txFpRowStream.stream(rowTxId, rowFP, rowType, row))),
            (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row) -> {
                HeapFiler filer = HeapFiler.fromBytes(value, value.length);
                byte[] deltaRow = primaryRowMarshaller.toRow(rowType,
                    key,
                    UIO.readByteArray(filer, "value", intBuffer),
                    valueTimestamp,
                    valueTombstoned,
                    valueVersion);
                if (!rowStream.row(fp, txId, rowType, deltaRow)) {
                    return false;
                }
                if (UIO.readBoolean(filer, "hasHighwaterHints")) {
                    if (!rowStream.row(-1, -1, RowType.highwater, UIO.readByteArray(filer, "highwaterHints", intBuffer))) {
                        return false;
                    }
                }
                return true;
            }));
    }

    @Override
    public WALValue hydrate(long fp) throws Exception {
        try {
            byte[] typeByteTxIdAndRow = wal.tx(io -> io.readTypeByteTxIdAndRow(fp));
            RowType rowType = RowType.fromByte(typeByteTxIdAndRow[0]);
            byte[] value = primaryRowMarshaller.valueFromRow(rowType, typeByteTxIdAndRow, 1 + 8);
            byte[] deltaValue = UIO.readByteArray(value, 0, "value");
            return new WALValue(rowType,
                deltaValue,
                primaryRowMarshaller.timestampFromRow(typeByteTxIdAndRow, 1 + 8),
                primaryRowMarshaller.tombstonedFromRow(typeByteTxIdAndRow, 1 + 8),
                primaryRowMarshaller.versionFromRow(typeByteTxIdAndRow, 1 + 8)
            );
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate fp:" + fp + ", WAL length:" + wal.length(), x);
        }
    }

    @Override
    public boolean hydrate(Fps fps, FpKeyValueStream fpKeyValueStream) throws Exception {
        try {
            return WALKey.decompose(
                decomposeStream -> {
                    return primaryRowMarshaller.fromRows(fpRowStream -> wal.tx(
                        io -> io.read(fps, (rowFP, rowTxId, rowType, row) -> fpRowStream.stream(rowFP, rowType, row))),
                        (fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                            byte[] deltaValue = UIO.readByteArray(value, 0, "value");
                            return decomposeStream.stream(-1, fp, rowType, key, deltaValue, valueTimestamp, valueTombstoned, valueVersion, null);
                        });
                },
                (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, entry) -> {
                    return fpKeyValueStream.stream(fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
                });
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrate fps, WAL length:" + wal.length(), x);
        }
    }

    public boolean hydrateKeyValueHighwaters(Fps fps, FpKeyValueHighwaterStream stream) throws Exception {
        byte[] intBuffer = new byte[4];
        return primaryRowMarshaller.fromRows(
            fpRowStream -> wal.tx(
                io -> io.read(fps,
                    (rowFP, rowTxId, rowType, row) -> fpRowStream.stream(rowTxId, rowFP, rowType, row))),
            (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row) -> {
                try {
                    HeapFiler filer = HeapFiler.fromBytes(value, value.length);
                    byte[] hydrateValue = UIO.readByteArray(filer, "value", intBuffer);
                    WALHighwater highwater = null;
                    if (UIO.readBoolean(filer, "hasHighwaterHint")) {
                        highwater = highwaterRowMarshaller.fromBytes(UIO.readByteArray(filer, "highwaters", intBuffer));
                    }
                    return stream.stream(fp, rowType, prefix, key, hydrateValue, valueTimestamp, valueTombstoned, valueVersion, highwater);
                } catch (Exception x) {
                    throw new RuntimeException("Failed to hydrate fp:" + fp + " length:" + wal.length(), x);
                }
            });
    }

    void destroy() throws Exception {
        synchronized (oneTxAtATimeLock) {
            wal.delete();
        }
    }

    @Override
    public int compareTo(DeltaWAL o) {
        return Long.compare(id, o.id);
    }

    public static class KeyValueHighwater {

        public final RowType rowType;
        public final byte[] prefix;
        public final byte[] key;
        public final byte[] value;
        public final long valueTimestamp;
        public final boolean valueTombstone;
        public final long valueVersion;
        public final WALHighwater highwater;

        public KeyValueHighwater(RowType rowType,
            byte[] prefix,
            byte[] key,
            byte[] value,
            long valueTimestamp,
            boolean valueTombstone,
            long valueVersion,
            WALHighwater highwater) {
            this.rowType = rowType;
            this.prefix = prefix;
            this.key = key;
            this.value = value;
            this.valueTimestamp = valueTimestamp;
            this.valueTombstone = valueTombstone;
            this.valueVersion = valueVersion;
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
