package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.TimestampedValue;
import java.util.Map;

public interface TransactionEntry<K, V> extends Map.Entry<K, TimestampedValue<V>> {

    long getOrderId();

}
