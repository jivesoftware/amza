package com.jivesoftware.os.amza.service.collections;

import java.util.concurrent.atomic.AtomicReference;

/*
 * Copied from github.com/jnthnclt/nicity
 *
 * CSet.java.java
 *
 * Created on 03-12-2010 10:52:02 PM
 *
 * Copyright 2010 Jonathan Colt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 * @author Administrator
 * @param <K>
 */
public class BAHash<V> {

    private static final int MIN_KEYS = 5;
    private static final Object SKIP = new Object();

    private final Hasher<byte[]> hasher;
    private final Equaler<byte[]> equaler;
    private final boolean hasValues;
    private final AtomicReference<State> state;

    private static final class State {

        final int maxKeys;
        final int maxCapacity;
        final Object[] keys;
        final Object[] values;
        int numKeys;

        public State(int maxKeys, int numKeys, int maxCapacity, Object[] keys, Object[] values) {
            this.maxKeys = maxKeys;
            this.numKeys = numKeys;
            this.maxCapacity = maxCapacity;
            this.keys = keys;
            this.values = values;
        }

    }

    /**
     *
     * @param capacity
     */
    public BAHash(int capacity, Hasher<byte[]> hasher, Equaler<byte[]> equaler, boolean hasValues) {
        this.hasher = hasher;
        this.equaler = equaler;
        this.hasValues = hasValues;
        this.state = new AtomicReference<>(allocate(capacity, hasValues));
    }

    private State allocate(int capacity, boolean hasValues) {

        int numKeys = 0;
        int maxKeys;
        int maxCapacity;
        if (capacity <= MIN_KEYS) {
            maxKeys = MIN_KEYS;
            maxCapacity = MIN_KEYS * 2;
        } else {
            maxKeys = capacity;
            maxCapacity = capacity * 2;
        }
        Object[] keys = new Object[maxCapacity];
        Object[] values = (hasValues) ? new Object[maxCapacity] : keys;
        return new State(maxKeys, numKeys, maxCapacity, keys, values);

    }

    public int size() {
        return state.get().maxCapacity;
    }

    public long getCount() {
        return state.get().maxKeys;
    }

    /**
     *
     * @return
     */
    public void clear() {
        state.set(allocate(MIN_KEYS, hasValues));
    }

    private int hash(State state, int keyShuffle) {
        keyShuffle += keyShuffle >> 8; // shuffle bits to avoid worst case clustering

        if (keyShuffle < 0) {
            keyShuffle = -keyShuffle;
        }
        return keyShuffle % state.maxCapacity;
    }

    public V get(byte[] key, int keyOffset, int keyLength) {
        return get(hasher.hashCode(key, keyOffset, keyLength), key, keyOffset, keyLength);
    }

    public V get(int hashCode, byte[] key, int keyOffset, int keyLength) {
        if (key == null || key == SKIP) {
            return null;
        }
        State s = state.get();
        if (s.maxKeys == 0) {
            return null;
        }
        int start = hash(s, hashCode);
        for (int i = start, j = 0, k = s.maxCapacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            if (s.keys[i] == SKIP) {
                continue;
            }
            if (s.keys[i] == null) {
                return null;
            }
            if (equaler.equals((byte[]) s.keys[i], key, keyOffset, keyLength)) {
                return (V) s.values[i];
            }
        }
        return null;
    }

    public void remove(byte[] key, int keyOffset, int keyLength) {
        remove(hasher.hashCode(key, keyOffset, keyLength), key, keyOffset, keyLength);
    }

    @SuppressWarnings("unchecked")
    public void remove(int hashCode, byte[] key, int keyOffset, int keyLength) {
        if (key == null || key == SKIP) {
            return;
        }
        State s = state.get();
        if (s.maxKeys == 0) {
            return;
        }
        int start = hash(s, hashCode);
        for (int i = start, j = 0, k = s.maxCapacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) {					// wraps around table

            if (s.keys[i] == SKIP) {
                continue;
            }
            if (s.keys[i] == null) {
                return;
            }
            if (equaler.equals((byte[]) s.keys[i], key, keyOffset, keyLength)) {
                int next = (i + 1) % k;
                if (s.keys[next] == null) {
                    for (int z = i; z >= 0; z--) {
                        if (s.keys[z] != SKIP) {
                            break;
                        }
                        s.keys[z] = null;
                    }
                    s.keys[i] = null;
                } else {
                    s.keys[i] = SKIP;
                }
                s.numKeys--;
                return;
            }
        }
    }

    public void put(byte[] key, int keyOffset, int keyLength, V value) {
        put(hasher.hashCode(key, keyOffset, keyLength), key, keyOffset, keyLength, value);
    }

    @SuppressWarnings("unchecked")
    public void put(int hashCode, byte[] key, int keyOffset, int keyLength, V value) {
        State s = state.get();
        if (s.numKeys >= s.maxKeys) {
            s = grow();
        }
        int start = hash(s, hashCode);
        for (int i = start, j = 0, k = s.maxCapacity; // stack vars for efficiency
            j < k; // max search for available slot
            i = (++i) % k, j++) {					// wraps around table

            if (s.keys[i] == key) {
                s.keys[i] = key;
                s.values[i] = value;
                return;
            }
            if (s.keys[i] == null || s.keys[i] == SKIP) {
                s.keys[i] = key;
                s.values[i] = value;
                s.numKeys++;
                return;
            }
            if (equaler.equals((byte[]) s.keys[i], key, keyOffset, keyLength)) {
                s.keys[i] = key;
                s.values[i] = value;
                return;
            }
        }
    }

    private State grow() {
        State s = state.get();
        BAHash<V> grown = new BAHash<>(s.maxKeys * 2, hasher, equaler, hasValues);
        for (int i = 0; i < s.keys.length; i++) {
            if (s.keys[i] == null || s.keys[i] == SKIP) {
                continue;
            }
            byte[] k = (byte[]) s.keys[i];
            grown.put(k, 0, k.length, (V) s.values[i]);
        }
        s = grown.state.get();
        state.set(s);
        return s;
    }

    public boolean stream(KeyValueStream<byte[], V> stream) throws Exception {
        State s = state.get();
        long c = s.maxCapacity;
        if (c <= 0) {
            return true;
        }
        for (int i = 0; i < c; i++) {
            byte[] k = (byte[]) s.keys[i];
            if (k == null || k == SKIP) {
                continue;
            }
            V value = (V) s.values[i];
            if (!stream.keyValue(k, value)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "HMap(" + getCount() + ")";
    }

}
