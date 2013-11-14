package com.jivesoftware.os.amza.shared;

import java.util.NavigableMap;

public class TransactionSet<K, V> {

    private final long highestTransactionId;
    private final NavigableMap<K, TimestampedValue<V>> changes;

    public TransactionSet(long highestTransactionId, NavigableMap<K, TimestampedValue<V>> changes) {
        this.highestTransactionId = highestTransactionId;
        this.changes = changes;
    }

    public long getHighestTransactionId() {
        return highestTransactionId;
    }

    public NavigableMap<K, TimestampedValue<V>> getChanges() {
        return changes;
    }

    @Override
    public String toString() {
        return "TransactionSet{" + "highestTransactionId=" + highestTransactionId + ", changes=" + changes + '}';
    }
}
