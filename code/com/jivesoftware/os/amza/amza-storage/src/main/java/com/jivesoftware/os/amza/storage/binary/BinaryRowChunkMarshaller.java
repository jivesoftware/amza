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
package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.storage.FstMarshaller;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.TransactionEntry;
import com.jivesoftware.os.amza.storage.chunks.ChunkFiler;
import com.jivesoftware.os.amza.storage.chunks.SubFiler;
import com.jivesoftware.os.amza.storage.chunks.UIO;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class BinaryRowChunkMarshaller<K, V> implements RowMarshaller<K, V, byte[]>, Serializable {

    private static final FstMarshaller FST_MARSHALLER = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());

    static {
        FST_MARSHALLER.registerSerializer(BinaryRow.class, new FSTBinaryRowMarshaller());
    }

    private final TableName<K, V> tableName;
    private final ChunkFiler chunkFiler;

    public BinaryRowChunkMarshaller(File workingDirectory, TableName<K, V> tableName) throws Exception {
        this.tableName = tableName;
        chunkFiler = ChunkFiler.factory(workingDirectory, "values-" + tableName.getTableName());
    }

    @Override
    public TableName<K, V> getTableName() {
        return tableName;
    }

    @Override
    public byte[] toRow(long orderId, Map.Entry<K, TimestampedValue<V>> e) throws Exception {

        byte[] valueAsBytes;
        try {
            valueAsBytes = FST_MARSHALLER.serialize(e.getValue().getValue());
        } catch (IOException x) {
            throw new IOException("Failed to serialize " + e.getValue().getValue().getClass(), x);
        }
        long chunkId;
        try {
            chunkId = chunkFiler.newChunk(valueAsBytes.length);
            SubFiler filer = chunkFiler.getFiler(chunkId);
            filer.setBytes(valueAsBytes);
            filer.flush();
        } catch (IOException x) {
            throw new IOException("Failed to save value to chuck filer. " + e.getValue().getValue().getClass(), x);
        }

        return FST_MARSHALLER.serialize(new BinaryRow(orderId,
                FST_MARSHALLER.serialize(e.getKey()),
                e.getValue().getTimestamp(),
                e.getValue().getTombstoned(),
                UIO.longBytes(chunkId)));
    }

    @Override
    public TransactionEntry<K, V> fromRow(byte[] row) throws Exception {
        try {
            BinaryRow binaryRow = FST_MARSHALLER.deserialize(row, BinaryRow.class);
            return new BinaryLazyLoadingTransactionEntry<>(binaryRow);
        } catch (Exception x) {
            throw new IOException("Failed deserialize row:" + row.length
                    + "bytes for table:" + tableName.getTableName()
                    + " ring:" + tableName.getRingName(), x);
        }
    }

    public class BinaryLazyLoadingTimestampValue<V> implements TimestampedValue<V> {
        private final long timestamp;
        private final boolean tombstone;
        private final long chunkId;

        public BinaryLazyLoadingTimestampValue(long timestamp, boolean tombstone, long chunkId) {
            this.timestamp = timestamp;
            this.tombstone = tombstone;
            this.chunkId = chunkId;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public boolean getTombstoned() {
            return tombstone;
        }

        @Override
        public V getValue() {
            byte[] rawValue = null;
            try {
                SubFiler filer = chunkFiler.getFiler(chunkId);
                rawValue = filer.toBytes();
            } catch (Exception x) {
                throw new RuntimeException("Unable to read value from chunkFiler for chunkId:" + chunkId, x);
            }
            try {
                return FST_MARSHALLER.deserialize(rawValue, (Class<V>) tableName.getValueClass());
            } catch (Exception x) {
                throw new RuntimeException("Failed to deserialize value. " + tableName.getValueClass(), x);
            }
        }

    }

    public class BinaryLazyLoadingTransactionEntry<K, V> implements TransactionEntry<K, V> {

        private final BinaryRow binaryRow;

        BinaryLazyLoadingTransactionEntry(BinaryRow binaryRow) {
            this.binaryRow = binaryRow;
        }

        @Override
        public long getOrderId() {
            return binaryRow.transaction;
        }

        @Override
        public K getKey() {
            try {
                return FST_MARSHALLER.deserialize(binaryRow.key, (Class<K>) tableName.getKeyClass());
            } catch (Exception x) {
                throw new RuntimeException("Failed to deserialize key. " + tableName.getKeyClass(), x);
            }
        }

        @Override
        public TimestampedValue<V> getValue() {
            return new BinaryLazyLoadingTimestampValue<>(binaryRow.timestamp, binaryRow.tombstone, UIO.bytesLong(binaryRow.value));
        }

        @Override
        public TimestampedValue<V> setValue(TimestampedValue<V> value) {
            throw new UnsupportedOperationException("Will never be supported.");
        }

    }

}
