package com.jivesoftware.os.amza.shared;

import com.google.common.collect.Multimap;
import java.util.NavigableMap;

public class TableDelta<K, V> {

    private final NavigableMap<K, TimestampedValue<V>> applyMap;
    private final NavigableMap<K, TimestampedValue<V>> removeMap;
    private final Multimap<K, TimestampedValue<V>> clobberedMap;

    public TableDelta(NavigableMap<K, TimestampedValue<V>> applyMap,
            NavigableMap<K, TimestampedValue<V>> removeMap,
            Multimap<K, TimestampedValue<V>> clobberedMap) {
        this.applyMap = applyMap;
        this.removeMap = removeMap;
        this.clobberedMap = clobberedMap;
    }

    public NavigableMap<K, TimestampedValue<V>> getApply() {
        return applyMap;
    }

    public NavigableMap<K, TimestampedValue<V>> getRemove() {
        return removeMap;
    }

    public Multimap<K, TimestampedValue<V>> getClobbered() {
        return clobberedMap;
    }
}