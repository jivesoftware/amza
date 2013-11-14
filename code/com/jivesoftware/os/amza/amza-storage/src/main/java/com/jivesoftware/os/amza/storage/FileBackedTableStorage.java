package com.jivesoftware.os.amza.storage;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class FileBackedTableStorage<K, V> implements TableStorage<K, V> {

    private final RowTableFile<K, V, String> rowTableFile;

    public FileBackedTableStorage(RowTableFile<K, V, String> rowTableFile) {
        this.rowTableFile = rowTableFile;
    }

    @Override
    public TableName<K, V> getTableName() {
        return rowTableFile.getTableName();
    }

    @Override
    synchronized public ConcurrentNavigableMap<K, TimestampedValue<V>> load() throws Exception {
        return rowTableFile.load();
    }

    @Override
    public void rowMutationSince(final long transactionId, TransactionSetStream<K, V> transactionSetStream) throws Exception {
        rowTableFile.rowMutationSince(transactionId, transactionSetStream);
    }

    @Override
    synchronized public void clear() throws Exception {
        ConcurrentNavigableMap<K, TimestampedValue<V>> saveableMap = new ConcurrentSkipListMap<>();
        Multimap<K, TimestampedValue<V>> all = ArrayListMultimap.create();
        ConcurrentNavigableMap<K, TimestampedValue<V>> load = load();
        for (Entry<K, TimestampedValue<V>> entry : load.entrySet()) {
            all.put(entry.getKey(), entry.getValue());
        }
        rowTableFile.save(all, saveableMap, false);
    }

    @Override
    synchronized public TableDelta<K, V> update(NavigableMap<K, TimestampedValue<V>> mutatedRows,
            ConcurrentNavigableMap<K, TimestampedValue<V>> allRows) throws Exception {

        NavigableMap<K, TimestampedValue<V>> applyMap = new TreeMap<>();
        NavigableMap<K, TimestampedValue<V>> removeMap = new TreeMap<>();
        Multimap<K, TimestampedValue<V>> clobberedRows = ArrayListMultimap.create();

        for (Entry<K, TimestampedValue<V>> e : mutatedRows.entrySet()) {
            K key = e.getKey();
            TimestampedValue<V> update = e.getValue();
            TimestampedValue<V> current = allRows.get(key);
            if (current == null) {
                applyMap.put(key, update);
            } else {
                if (update.getTombstoned() && update.getTimestamp() < 0) { // Handle tombstone updates
                    if (current.getTimestamp() <= Math.abs(update.getTimestamp())) {
                        TimestampedValue<V> removeable = allRows.get(key);
                        if (removeable != null) {
                            removeMap.put(key, removeable);
                            clobberedRows.put(key, removeable);
                        }
                    }
                } else if (current.getTimestamp() < update.getTimestamp()) {
                    clobberedRows.put(key, current);
                    applyMap.put(key, update);
                }
            }
        }
        if (!applyMap.isEmpty()) {
            rowTableFile.save(clobberedRows, applyMap, true);
        }
        return new TableDelta<>(applyMap, removeMap, clobberedRows);
    }

    @Override
    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        rowTableFile.compactTombestone(ifOlderThanNMillis);
    }
}