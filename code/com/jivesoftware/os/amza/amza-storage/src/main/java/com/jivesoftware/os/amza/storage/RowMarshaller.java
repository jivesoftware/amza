package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import java.util.Map;

public interface RowMarshaller<K, V, R> {

    TableName<K, V> getTableName();

    R toRow(long orderId, Map.Entry<K, TimestampedValue<V>> e) throws Exception;

    TransactionEntry<K, V> fromRow(R line) throws Exception;
}
