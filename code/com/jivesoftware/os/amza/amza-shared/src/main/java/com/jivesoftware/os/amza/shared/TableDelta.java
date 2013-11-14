package com.jivesoftware.os.amza.shared;

import com.google.common.collect.Multimap;
import java.util.concurrent.ConcurrentNavigableMap;

public class TableDelta<K, V> {

    private final ConcurrentNavigableMap<K, TimestampedValue<V>> appliedRows;
    private final Multimap<K, TimestampedValue<V>> oldRows;
    private final ConcurrentNavigableMap<K , TimestampedValue<V>> readableRows;

    public TableDelta(ConcurrentNavigableMap<K, TimestampedValue<V>> appliedRows,
            Multimap<K, TimestampedValue<V>> oldRows,
            ConcurrentNavigableMap<K, TimestampedValue<V>> readableKVT) {
        this.appliedRows = appliedRows;
        this.oldRows = oldRows;
        this.readableRows = readableKVT;
    }

    public ConcurrentNavigableMap<K, TimestampedValue<V>> getAppliedRows() {
        return appliedRows;
    }

    public Multimap<K, TimestampedValue<V>> getOldRows() {
        return oldRows;
    }

    public ConcurrentNavigableMap<K, TimestampedValue<V>> getReadableRows() {
        return readableRows;
    }
}