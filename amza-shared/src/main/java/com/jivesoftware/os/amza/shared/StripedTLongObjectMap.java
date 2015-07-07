package com.jivesoftware.os.amza.shared;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

/**
 *
 */
public class StripedTLongObjectMap<V> {

    private final TLongObjectMap<V>[] maps;

    public StripedTLongObjectMap(int concurrencyLevel) {
        TLongObjectMap[] maps = new TLongObjectMap[concurrencyLevel];
        for (int i = 0; i < maps.length; i++) {
            maps[i] = new TLongObjectHashMap<>();
        }
        this.maps = maps;
    }

    public V put(long key, V v) {
        TLongObjectMap<V> map = map(key);
        synchronized (map) {
            return map.put(key, v);
        }
    }

    public V get(long key) {
        TLongObjectMap<V> map = map(key);
        synchronized (map) {
            return map.get(key);
        }
    }

    public V remove(long key) {
        TLongObjectMap<V> map = map(key);
        synchronized (map) {
            return map.remove(key);
        }
    }

    private TLongObjectMap<V> map(long key) {
        return maps[Math.abs(Long.hashCode(key) % maps.length)];
    }
}
