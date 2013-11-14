package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.shared.KeyValueFilter;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TableStore<K, V> {

    private final ReadWriteTableStore<K, V> readWriteMaps;

    public TableStore(ReadWriteTableStore<K, V> readWriteTable) {
        this.readWriteMaps = readWriteTable;
    }

    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        readWriteMaps.compactTombestone(ifOlderThanNMillis);
    }

    public ConcurrentNavigableMap<K, TimestampedValue<V>> filter(KeyValueFilter<K, V> filter) throws Exception {
        ConcurrentNavigableMap<K, TimestampedValue<V>> results = filter.createCollector();
        for (Map.Entry<K, TimestampedValue<V>> e : readWriteMaps.getImmutableCopy().entrySet()) {
            if (e.getValue().getTombstoned()) {
                continue;
            }
            if (filter.filter(e.getKey(), e.getValue().getValue())) {
                results.put(e.getKey(), e.getValue());
            }
        }
        return results;
    }

    public V getValue(K k) throws Exception {
        return readWriteMaps.get(k);
    }

    public TimestampedValue<V> getTimestampedValue(K k) throws Exception {
        return readWriteMaps.getTimestampedValue(k);
    }

    public NavigableMap<K, TimestampedValue<V>> getImmutableRows() throws Exception {
        return readWriteMaps.getImmutableCopy();
    }

    public void getMutatedRowsSince(long transactionId, TransactionSetStream<K, V> transactionSetStream) throws Exception {
        readWriteMaps.getMutatedRowsSince(transactionId, transactionSetStream);
    }

    public void clearAllRows() throws Exception {
        readWriteMaps.clear();
    }

    public void commit(NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        readWriteMaps.commit(changes);
    }

    public TableTransaction<K, V> startTransaction(long timestamp) throws Exception {
        ConcurrentSkipListMap<K, TimestampedValue<V>> copy = readWriteMaps.getCopy();
        IsolatedChanges<K, V> isolatedChangesMap = new IsolatedChanges<>(copy, timestamp);
        return new TableTransaction<>(this, isolatedChangesMap);
    }
}