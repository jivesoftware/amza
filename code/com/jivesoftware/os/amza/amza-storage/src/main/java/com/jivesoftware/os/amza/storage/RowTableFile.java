package com.jivesoftware.os.amza.storage;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableRowReader;
import com.jivesoftware.os.amza.shared.TableRowWriter;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.commons.lang.mutable.MutableLong;

public class RowTableFile<K, V, R> {

    private final OrderIdProvider orderIdProvider;
    private final RowMarshaller<K, V, R> rowMarshaller;
    private final TableRowReader<R> rowReader;
    private final TableRowWriter<R> rowWriter;

    public RowTableFile(OrderIdProvider orderIdProvider,
            RowMarshaller<K, V, R> rowMarshaller,
            TableRowReader<R> rowReader,
            TableRowWriter<R> rowWriter) {
        this.orderIdProvider = orderIdProvider;
        this.rowMarshaller = rowMarshaller;
        this.rowReader = rowReader;
        this.rowWriter = rowWriter;
    }

    public TableName<K, V> getTableName() {
        return rowMarshaller.getTableName();
    }

    synchronized public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        //System.out.println("TODO: compactTombestone ifOlderThanNMillis:" + ifOlderThanNMillis);
//        long[] unpack = new AmzaChangeIdPacker().unpack(ifOlderThanNMillis);
        //        ConcurrentNavigableMap<K, TimestampedValue<V>> changes = new ConcurrentSkipListMap<>();
//        ConcurrentNavigableMap<K, TimestampedValue<V>> currentMap = readMap.get();
//        for (Map.Entry<K, TimestampedValue<V>> e : currentMap.entrySet()) {
//            TimestampedValue<V> timestampedValue = e.getValue();
//            if (timestampedValue.getTimestamp() < ifOlderThanTimestamp) {
//                changes.put(e.getKey(), new TimestampedValue<V>(null, -timestampedValue.getTimestamp(), true));
//            }
//        }
//        if (!changes.isEmpty()) {
//            applyChanges(changes);
//        }
    }

    synchronized public void save(Multimap<K, TimestampedValue<V>> was, NavigableMap<K, TimestampedValue<V>> mutation, boolean append) throws Exception {
        long transactionId = 0;
        if (orderIdProvider != null) {
            transactionId = orderIdProvider.nextId();
        }
        List<R> rows = new ArrayList<>();
        for (Map.Entry<K, TimestampedValue<V>> e : mutation.entrySet()) {
            rows.add(rowMarshaller.toRow(transactionId, e));
        }
        rowWriter.write(rows, append);
    }

    synchronized public ConcurrentNavigableMap<K, TimestampedValue<V>> load() throws Exception {
        final ConcurrentSkipListMap<K, TimestampedValue<V>> table = new ConcurrentSkipListMap<>();
        final Multimap<K, TimestampedValue<V>> was = ArrayListMultimap.create();
        rowReader.read(false, new TableRowReader.Stream<R>() {
            @Override
            public boolean stream(R row) throws Exception {
                TransactionEntry<K, V> transactionEntry = rowMarshaller.fromRow(row);
                K key = transactionEntry.getKey();
                TimestampedValue<V> timestampedValue = transactionEntry.getValue();
                TimestampedValue<V> current = table.get(key);
                if (current == null) {
                    table.put(key, timestampedValue);
                } else if (current.getTimestamp() < timestampedValue.getTimestamp()) {
                    was.put(key, current);
                    table.put(key, timestampedValue);
                }
                return true;
            }
        });
        save(was, table, false); // write compacted table after load.
        return table;
    }

    synchronized public void rowMutationSince(final long transactionId, TransactionSetStream<K, V> transactionSetStream) throws Exception {
        final MutableLong higestOrderId = new MutableLong(transactionId);
        final ConcurrentSkipListMap<K, TimestampedValue<V>> changes = new ConcurrentSkipListMap<>();
        rowReader.read(true, new TableRowReader.Stream<R>() {
            @Override
            public boolean stream(R row) throws Exception {
                TransactionEntry<K, V> transactionEntry = rowMarshaller.fromRow(row);
                if (transactionEntry.getOrderId() <= transactionId) {
                    return false;
                }
                TimestampedValue<V> had = changes.putIfAbsent(transactionEntry.getKey(), transactionEntry.getValue());
                if (had != null && had.getTimestamp() > transactionEntry.getValue().getTimestamp()) {
                    changes.put(transactionEntry.getKey(), had);
                }
                higestOrderId.setValue(Math.max(higestOrderId.longValue(), transactionEntry.getOrderId()));
                return true;
            }
        });
        transactionSetStream.stream(new TransactionSet<>(higestOrderId.longValue(), changes));
    }
}