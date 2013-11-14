package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.TimestampedValue;

public class BasicTransactionEntry<K, V> implements TransactionEntry<K, V> {

    private final long orderId;
    private final K key;
    private final TimestampedValue<V> value;

    public BasicTransactionEntry(long orderId, K key, TimestampedValue<V> value) {
        this.orderId = orderId;
        this.key = key;
        this.value = value;
    }

    @Override
    public long getOrderId() {
        return orderId;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public TimestampedValue<V> getValue() {
        return value;
    }

    @Override
    public TimestampedValue<V> setValue(TimestampedValue<V> value) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public String toString() {
        return "BasicTransactionEntry{" + "orderId=" + orderId + ", key=" + key + ", value=" + value + '}';
    }
}