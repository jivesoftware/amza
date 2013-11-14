package com.jivesoftware.os.amza.shared;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;

public interface TableStorage<K, V> {

    TableName<K, V> getTableName();

    ConcurrentNavigableMap<K, TimestampedValue<V>> load() throws Exception;

    TableDelta<K, V> update(NavigableMap<K, TimestampedValue<V>> mutatedRows, ConcurrentNavigableMap<K, TimestampedValue<V>> allRows) throws Exception;

    void rowMutationSince(final long transactionId, TransactionSetStream<K, V> transactionSetStream) throws Exception;

    void compactTombestone(long ifOlderThanNMillis) throws Exception;

    void clear() throws Exception;
}