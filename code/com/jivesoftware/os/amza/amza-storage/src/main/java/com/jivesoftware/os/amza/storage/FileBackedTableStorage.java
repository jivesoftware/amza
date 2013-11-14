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

        ConcurrentNavigableMap<K, TimestampedValue<V>> saveableMap = new ConcurrentSkipListMap<>();
        saveableMap.putAll(allRows);

        ConcurrentNavigableMap<K, TimestampedValue<V>> appliedRows = new ConcurrentSkipListMap<>();
        Multimap<K, TimestampedValue<V>> oldRows = ArrayListMultimap.create();
        ConcurrentNavigableMap<K, TimestampedValue<V>> trumpingRows = new ConcurrentSkipListMap<>();

        for (Entry<K, TimestampedValue<V>> e : mutatedRows.entrySet()) {
            K key = e.getKey();
            TimestampedValue<V> update = e.getValue();
            TimestampedValue<V> current = saveableMap.get(key);
            if (current == null) {
                saveableMap.put(key, update);
                appliedRows.put(key, update);
            } else {
                if (update.getTombstoned() && update.getTimestamp() < 0) { // Handle tombstone updates
                    if (current.getTimestamp() <= Math.abs(update.getTimestamp())) {
                        TimestampedValue<V> remove = saveableMap.remove(key);
                        if (remove != null) {
                            oldRows.put(key, remove);
                        }
                        appliedRows.put(key, update);
                    } else {
                        trumpingRows.put(key, current);
                    }
                } else if (current.getTimestamp() < update.getTimestamp()) {
                    oldRows.put(key, current);
                    saveableMap.put(key, update);
                    appliedRows.put(key, update);
                } else {
                    trumpingRows.put(key, current);
                }
            }
        }
        if (appliedRows.isEmpty()) {
            //System.out.println("Current:" + getTableName() + " appliedRows:" + appliedRows.size() + " trumpingRows:" + trumpingRows.size());
            return new TableDelta<>(appliedRows, oldRows, allRows);
        }
        //System.out.println("Saved:" + getTableName() + " appliedRows:" + appliedRows.size() + " trumpingRows:" + trumpingRows.size());
        rowTableFile.save(oldRows, appliedRows, true);
        return new TableDelta<>(appliedRows, oldRows, saveableMap);
    }

    @Override
    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        rowTableFile.compactTombestone(ifOlderThanNMillis);
    }
}