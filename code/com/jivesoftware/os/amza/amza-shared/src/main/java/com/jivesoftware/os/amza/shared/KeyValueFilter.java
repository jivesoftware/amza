package com.jivesoftware.os.amza.shared;

import java.util.concurrent.ConcurrentNavigableMap;

public interface KeyValueFilter<K, V> {

    ConcurrentNavigableMap<K, TimestampedValue<V>> createCollector();

    boolean filter(K key, V value);

    void reset();
}