package com.jivesoftware.os.amza.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.storage.chunks.ChunkFiler;
import com.jivesoftware.os.amza.storage.chunks.SubFiler;
import java.nio.charset.Charset;

public class LazyLoadingTransactionEntry<K, V> implements TransactionEntry<K, V> {

    private final static Charset UTF8 = Charset.forName("UTF-8");

    private final long orderId;
    private final K key;
    private final ObjectMapper mapper;
    private final ChunkFiler chunkFiler;
    private final long chunkId;
    private final Class<V> valueClass;
    private final long timestamp;
    private final boolean tombstoned;

    public LazyLoadingTransactionEntry(long orderId, K key,
            ObjectMapper mapper, ChunkFiler chunkFiler, long chunkId, Class<V> valueClass, long timestamp, boolean tombstoned) {
        this.orderId = orderId;
        this.key = key;
        this.chunkFiler = chunkFiler;
        this.mapper = mapper;
        this.chunkId = chunkId;
        this.valueClass = valueClass;
        this.timestamp = timestamp;
        this.tombstoned = tombstoned;
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

        try {
            SubFiler filer = chunkFiler.getFiler(chunkId);
            String valueString = new String(filer.toBytes(), UTF8);
            V value = mapper.readValue(valueString, valueClass);
            return new TimestampedValue<>(value, timestamp, tombstoned);
        } catch (Exception x) {
            throw new RuntimeException("Unable to read value from chunkFiler for chunkId:" + chunkId, x);
        }
    }

    @Override
    public TimestampedValue<V> setValue(TimestampedValue<V> value) {
        throw new UnsupportedOperationException("Not supported.");
    }

}
