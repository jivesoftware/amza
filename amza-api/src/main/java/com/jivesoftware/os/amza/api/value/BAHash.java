package com.jivesoftware.os.amza.api.value;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class BAHash<V> implements BAH<V> {

    private final Hasher<byte[]> hasher;
    private final Equaler<byte[]> equaler;
    private final AtomicReference<BAHState<byte[], V>> state;

    /**
     *
     * @param capacity
     */
    public BAHash(BAHState<byte[], V> state, Hasher<byte[]> hasher, Equaler<byte[]> equaler) {
        this.hasher = hasher;
        this.equaler = equaler;
        this.state = new AtomicReference<>(state);
    }

    @Override
    public long size() {
        return state.get().size();
    }

    /**
     *
     * @return
     */
    @Override
    public void clear() {
        state.set(state.get().allocate(0));
    }

    private long hash(BAHState state, long keyShuffle) {
        keyShuffle += keyShuffle >> 8; // shuffle bits to avoid worst case clustering

        if (keyShuffle < 0) {
            keyShuffle = -keyShuffle;
        }
        return keyShuffle % state.capacity();
    }

    @Override
    public V get(byte[] key, int keyOffset, int keyLength) {
        return get(hasher.hashCode(key, keyOffset, keyLength), key, keyOffset, keyLength);
    }

    @Override
    public V get(long hashCode, byte[] key, int keyOffset, int keyLength) {
        BAHState<byte[], V> s = state.get();
        byte[] skipped = s.skipped();
        if (key == null || key == skipped) {
            return null;
        }
        if (s.size() == 0) {
            return null;
        }
        long capacity = s.capacity();
        long start = hash(s, hashCode);
        for (long i = start, j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            byte[] storedKey = s.key(i);
            if (storedKey == skipped) {
                continue;
            }
            if (storedKey == null) {
                return null;
            }
            if (equaler.equals(storedKey, key, keyOffset, keyLength)) {
                return s.value(i);
            }
        }
        return null;

    }

    @Override
    public void remove(byte[] key, int keyOffset, int keyLength) {
        remove(hasher.hashCode(key, keyOffset, keyLength), key, keyOffset, keyLength);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void remove(long hashCode, byte[] key, int keyOffset, int keyLength) {
        BAHState<byte[], V> s = state.get();
        byte[] skipped = s.skipped();
        if (key == null || key == skipped) {
            return;
        }
        if (s.size() == 0) {
            return;
        }
        long capacity = s.capacity();
        long start = hash(s, hashCode);
        for (long i = start, j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) {					// wraps around table

            byte[] storedKey = s.key(i);
            if (storedKey == skipped) {
                continue;
            }
            if (storedKey == null) {
                return;
            }

            if (equaler.equals(storedKey, key, keyOffset, keyLength)) {
                long next = (i + 1) % k;

                s.remove(i, skipped, null);
                if (s.key(next) == null) {
                    for (long z = i, y = 0; y < capacity; z = (z + capacity - 1) % k, y++) {
                        if (s.key(z) != skipped) {
                            break;
                        }
                        s.clear(z);
                    }
                }
                return;
            }
        }
    }

    @Override
    public void put(byte[] key, int keyOffset, int keyLength, V value) {
        put(hasher.hashCode(key, keyOffset, keyLength), key, keyOffset, keyLength, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void put(long hashCode, byte[] key, int keyOffset, int keyLength, V value) {
        BAHState<byte[], V> s = state.get();
        internalPut(s, hashCode, key, keyOffset, keyLength, value);
    }

    private void internalPut(BAHState<byte[], V> s, long hashCode, byte[] key, int keyOffset, int keyLength, V value) {
        long capacity = s.capacity();
        if (s.size() * 2 >= capacity) {
            s = grow();
            capacity = s.capacity();
        }
        long start = hash(s, hashCode);
        byte[] skipped = s.skipped();
        for (long i = start, j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for available slot
            i = (++i) % k, j++) {
            // wraps around table

            byte[] storedKey = s.key(i);
            if (storedKey == key) {
                s.link(i, key, value);
                return;
            }
            if (storedKey == null || storedKey == skipped) {
                s.link(i, key, value);
                return;
            }
            if (equaler.equals(storedKey, key, keyOffset, keyLength)) {
                s.update(i, key, value);
                return;
            }
        }
    }

    private BAHState<byte[], V> grow() {
        BAHState<byte[], V> s = state.get();
        BAHState<byte[], V> grown = s.allocate(s.capacity() * 2);
        long i = s.first();
        byte[] skipped = grown.skipped();
        while (i != -1) {
            byte[] storedKey = s.key(i);
            if (storedKey != null && storedKey != skipped) {
                long hash = hasher.hashCode(storedKey, 0, storedKey.length);
                internalPut(grown, hash, storedKey, 0, storedKey.length, s.value(i));
            }
            i = s.next(i);
        }
        state.set(grown);
        return grown;
    }

    @Override
    public boolean stream(KeyValueStream<byte[], V> stream) throws Exception {
        BAHState<byte[], V> s = state.get();
        long c = s.capacity();
        if (c <= 0) {
            return true;
        }
        byte[] skipped = s.skipped();
        long i = s.first();
        while (i != -1) {

            byte[] k = s.key(i);
            if (k != null && k != skipped) {
                V value = s.value(i);
                if (!stream.keyValue(k, value)) {
                    return false;
                }
            }
            i = s.next(i);
        }
        return true;
    }

    @Override
    public void dump() {
        BAHState<byte[], V> s = state.get();
        long c = s.capacity();
        if (c <= 0) {
            return;
        }
        byte[] skipped = s.skipped();
        long i = s.first();
        while (i != -1) {
            byte[] k = s.key(i);
            if (k != null && k != skipped) {
                V value = s.value(i);
                System.out.println(Arrays.toString(k) + " " + value);
            } else {
                System.out.println(((k == skipped) ? "SKIP" : "NULL") + " " + s.value(i));
            }
            i = s.next(i);
        }
    }

    @Override
    public String toString() {
        return "LinkedHMap(" + size() + ")";
    }

}
