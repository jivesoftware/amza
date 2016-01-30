package com.jivesoftware.os.amza.api.value;

import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class BAHLinkedMapState<K, V> implements BAHState<K, V> {

    public static final byte[] NIL = new byte[0];

    private final long capacity;
    private final boolean hasValues;
    private final K nilKey;

    private final Object[] keys;
    private long firstKeyIndex = -1;
    private final long[] priorKeyIndex;
    private final long[] nextKeyIndex;
    private long lastKeyIndex = -1;
    private final Object[] values;
    private int count;

    public BAHLinkedMapState(long capacity, boolean hasValues, K nilKey) {
        this.count = 0;
        this.capacity = capacity;
        this.hasValues = hasValues;
        this.nilKey = nilKey;

        this.keys = new Object[(int) capacity];
        this.firstKeyIndex = -1;
        this.priorKeyIndex = new long[(int) capacity];
        Arrays.fill(priorKeyIndex, -1);
        this.nextKeyIndex = new long[(int) capacity];
        Arrays.fill(nextKeyIndex, -1);
        this.lastKeyIndex = -1;
        this.values = (hasValues) ? new Object[(int) capacity] : keys;
    }

    @Override
    public BAHState<K, V> allocate(long capacity) {
        return new BAHLinkedMapState<>(capacity, hasValues, nilKey);
    }

    @Override
    public K skipped() {
        return nilKey;
    }

    @Override
    public long first() {
        return firstKeyIndex;
    }

    @Override
    public long size() {
        return count;
    }

    @Override
    public void update(long i, K key, V value) {
        keys[(int) i] = key;
        values[(int) i] = value;
    }

    @Override
    public void link(long i, K key, V value) {
        if (firstKeyIndex == -1) {
            firstKeyIndex = i;
        }
        priorKeyIndex[(int) i] = lastKeyIndex;
        if (lastKeyIndex != -1) {
            nextKeyIndex[(int) lastKeyIndex] = i;
        }
        nextKeyIndex[(int) i] = -1;
        lastKeyIndex = i;

        keys[(int) i] = key;
        values[(int) i] = value;
        count++;
    }

    @Override
    public void clear(long i) {
        keys[(int) i] = null;
        values[(int) i] = null;
    }

    @Override
    public void remove(long i, K key, V value) {
        if (i == firstKeyIndex) {
            firstKeyIndex = nextKeyIndex[(int) i];
        }

        if (i == lastKeyIndex) {
            lastKeyIndex = priorKeyIndex[(int) i];
        }

        long prior = priorKeyIndex[(int) i];
        long next = nextKeyIndex[(int) i];

        if (prior != -1) {
            nextKeyIndex[(int) prior] = next;
        }
        if (next != -1) {
            priorKeyIndex[(int) next] = prior;
        }

        nextKeyIndex[(int) i] = -1;
        priorKeyIndex[(int) i] = -1;

        keys[(int) i] = key;
        values[(int) i] = value;
        count--;
    }

    @Override
    public long next(long i) {
        return nextKeyIndex[(int) i];
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public K key(long i) {
        return (K) keys[(int) i];
    }

    @Override
    public V value(long i) {
        return (V) values[(int) i];
    }

}
