/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.shared.BasicTimestampedValue;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.storage.chunks.ChunkFiler;
import com.jivesoftware.os.amza.storage.chunks.SubFiler;
import java.io.IOException;

public class LazyLoadingTransactionEntry<K, V> implements TransactionEntry<K, V> {

    private final long orderId;
    private final K key;
    private final ObjectMapper mapper;
    private final ChunkFiler chunkFiler;
    private final long chunkId;
    private final Class<V> valueClass;
    private final long timestamp;
    private final boolean tombstoned;

    public LazyLoadingTransactionEntry(long orderId,
            K key,
            ObjectMapper mapper,
            ChunkFiler chunkFiler,
            long chunkId,
            Class<V> valueClass,
            long timestamp,
            boolean tombstoned) {
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
        byte[] rawValue = null;
        try {
            SubFiler filer = chunkFiler.getFiler(chunkId);
            rawValue = filer.toBytes();
        } catch (Exception x) {
            throw new RuntimeException("Unable to read value from chunkFiler for chunkId:" + chunkId, x);
        }
        try {
            V value = mapper.readValue(rawValue, valueClass);
            return new BasicTimestampedValue<>(value, timestamp, tombstoned);
        } catch (IOException x) {
            throw new RuntimeException("Unable map rawValue to valueClass=" + valueClass, x);
        }
    }

    @Override
    public TimestampedValue<V> setValue(TimestampedValue<V> value) {
        throw new UnsupportedOperationException("Will never be supported.");
    }

}