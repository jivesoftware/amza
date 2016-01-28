package com.jivesoftware.os.amza.service.collections;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *
 * @author jonathan.colt
 */
public class ConcurrentBAHash<V> {

    private final Hasher<byte[]> hasher;
    private final BAHash<V>[] hmaps;

    @SuppressWarnings("unchecked")
    public ConcurrentBAHash(int capacity, boolean hasValues, int concurrency) {
        this.hasher = BAHasher.SINGLETON;
        this.hmaps = new BAHash[concurrency];
        for (int i = 0; i < concurrency; i++) {
            this.hmaps[i] = new BAHash<>(capacity / concurrency, hasher, BAEqualer.SINGLETON, hasValues);
        }
    }

    public void put(byte[] key, V value) {
        put(key, 0, key.length, value);
    }

    private void put(byte[] key, int keyOffset, int keyLength, V value) {
        int hashCode = hasher.hashCode(key, keyOffset, keyLength);
        BAHash<V> hmap = hmap(hashCode);
        synchronized (hmap) {
            hmap.put(hashCode, key, keyOffset, keyLength, value);
        }
    }

    private BAHash<V> hmap(int hashCode) {
        return hmaps[Math.abs((hashCode) % hmaps.length)];
    }

    public V computeIfAbsent(byte[] key, Function<byte[], ? extends V> mappingFunction) {
        int hashCode = hasher.hashCode(key, 0, key.length);
        BAHash< V> hmap = hmap(hashCode);
        synchronized (hmap) {
            V value = hmap.get(hashCode, key, 0, key.length);
            if (value == null) {
                value = mappingFunction.apply(key);
                hmap.put(hashCode, key, 0, key.length, value);
            }
            return value;
        }
    }

    public V compute(byte[] key, BiFunction<byte[], ? super V, ? extends V> remappingFunction) {
        int hashCode = hasher.hashCode(key, 0, key.length);
        BAHash<V> hmap = hmap(hashCode);
        synchronized (hmap) {
            V value = hmap.get(hashCode, key, 0, key.length);
            V remapped = remappingFunction.apply(key, value);
            if (remapped != value) {
                value = remapped;
                hmap.put(hashCode, key, 0, key.length, value);
            }
            return value;
        }
    }

    public V get(byte[] key) {
        return get(key, 0, key.length);
    }

    public V get(byte[] key, int keyOffset, int keyLength) {
        int hashCode = hasher.hashCode(key, keyOffset, keyLength);
        BAHash<V> hmap = hmap(hashCode);
        synchronized (hmap) {
            return hmap.get(hashCode, key, keyOffset, keyLength);
        }
    }

    public void remove(byte[] key) {

    }

    public void remove(byte[] key, int keyOffset, int keyLength) {
        int hashCode = hasher.hashCode(key, keyOffset, keyLength);
        BAHash< V> hmap = hmap(hashCode);
        synchronized (hmap) {
            hmap.remove(hashCode, key, keyOffset, keyLength);
        }
    }

    public void clear() {
        for (BAHash< V> hmap : hmaps) {
            synchronized (hmap) {
                hmap.clear();
            }
        }
    }

    public int size() {
        int size = 0;
        for (BAHash<V> hmap : hmaps) {
            size += hmap.size();
        }
        return size;
    }

    public boolean stream(KeyValueStream<byte[], V> keyValueStream) throws Exception {
        for (BAHash<V> hmap : hmaps) {
            if (!hmap.stream(keyValueStream)) {
                return false;
            }
        }
        return true;
    }
}
