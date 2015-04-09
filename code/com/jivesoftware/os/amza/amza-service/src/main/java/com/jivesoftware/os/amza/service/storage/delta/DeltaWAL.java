package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.storage.RowMarshaller;
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
public class DeltaWAL implements Comparable<DeltaWAL> {

    private final long id;
    private final OrderIdProvider orderIdProvider;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final WALTx rowsTx;
    private final AtomicLong updateCount = new AtomicLong();
    private final Object oneTxAtATimeLock = new Object();

    public DeltaWAL(long id,
        OrderIdProvider orderIdProvider,
        RowMarshaller<byte[]> rowMarshaller,
        WALTx rowsTx) {
        this.id = id;
        this.orderIdProvider = orderIdProvider;
        this.rowMarshaller = rowMarshaller;
        this.rowsTx = rowsTx;
    }

    public void load(final RowStream rowStream) throws Exception {
        rowsTx.read(new WALTx.WALRead<Void>() {

            @Override
            public Void read(WALReader reader) throws Exception {
                reader.scan(0, rowStream);
                return null;
            }
        });
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

    public DeltaWALApplied update(final RegionName regionName, final Map<WALKey, WALValue> apply) throws Exception {
        final Map<WALKey, byte[]> keyToRowPointer = new HashMap<>();

        final MutableLong txId = new MutableLong();
        rowsTx.write(new WALTx.WALWrite<Void>() {
            @Override
            public Void write(WALWriter rowWriter) throws Exception {
                List<WALKey> keys = new ArrayList<>();
                List<byte[]> rawRows = new ArrayList<>();
                for (Map.Entry<WALKey, WALValue> e : apply.entrySet()) {
                    WALKey key = e.getKey();
                    WALValue value = e.getValue();
                    keys.add(key);
                    key = regionPrefixedKey(regionName, key);
                    rawRows.add(rowMarshaller.toRow(key, value));
                }
                long transactionId;
                List<byte[]> rowPointers;
                synchronized (oneTxAtATimeLock) {
                    transactionId = (orderIdProvider == null) ? 0 : orderIdProvider.nextId();
                    rowPointers = rowWriter.write(Collections.nCopies(rawRows.size(), transactionId),
                        Collections.nCopies(rawRows.size(), (byte) WALWriter.VERSION_1),
                        rawRows);
                }
                txId.setValue(transactionId);
                for (int i = 0; i < rowPointers.size(); i++) {
                    keyToRowPointer.put(keys.get(i), rowPointers.get(i));
                }
                return null;
            }
        });
        updateCount.addAndGet(apply.size());
        return new DeltaWALApplied(keyToRowPointer, txId.longValue());

    }

    void takeRows(final NavigableMap<Long, List<byte[]>> tailMap, final RowStream rowStream) throws Exception {
        rowsTx.read(new WALTx.WALRead<Void>() {

            @Override
            public Void read(WALReader reader) throws Exception {
                // reverse everything so highest FP is first, helps minimize mmap extensions
                for (Long key : tailMap.descendingKeySet()) {
                    List<byte[]> rowFPs = Lists.reverse(tailMap.get(key));
                    for (byte[] fp : rowFPs) {
                        byte[] rawRow = reader.read(UIO.bytesLong(fp));
                        RowMarshaller.WALRow row = rowMarshaller.fromRow(rawRow);
                        ByteBuffer bb = ByteBuffer.wrap(row.getKey().getKey());
                        byte[] regionNameBytes = new byte[bb.getShort()];
                        bb.get(regionNameBytes);
                        byte[] keyBytes = new byte[bb.getInt()];
                        bb.get(keyBytes);

                        if (!rowStream.row(UIO.bytesLong(fp), key, BinaryRowWriter.VERSION_1,
                            rowMarshaller.toRow(new WALKey(keyBytes), row.getValue()))) { // TODO Ah were to get rowType
                            return null;
                        }
                    }
                }
                return null;
            }
        });

    }

    WALValue hydrate(RegionName regionName, final WALPointer rowPointer) throws Exception {
        try {
            byte[] row = rowsTx.read(new WALTx.WALRead<byte[]>() {
                @Override
                public byte[] read(WALReader rowReader) throws Exception {
                    return rowReader.read(rowPointer.getFp());
                }
            });
            byte[] value = rowMarshaller.valueFromRow(row);
            return new WALValue(value,
                rowPointer.getTimestampId(),
                rowPointer.getTombstoned());
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrtate " + rowPointer, x);
        }
    }

    void destroy() throws Exception {
        synchronized (oneTxAtATimeLock) {
            rowsTx.delete(false);
        }
    }

    @Override
    public int compareTo(DeltaWAL o) {
        return Long.compare(id, o.id);
    }

    public static class DeltaWALApplied {

        public final Map<WALKey, byte[]> keyToRowPointer;
        public final long txId;

        public DeltaWALApplied(Map<WALKey, byte[]> keyToRowPointer, long txId) {
            this.keyToRowPointer = keyToRowPointer;
            this.txId = txId;
        }
    }
}
