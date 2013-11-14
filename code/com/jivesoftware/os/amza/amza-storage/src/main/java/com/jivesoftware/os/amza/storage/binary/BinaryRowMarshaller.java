package com.jivesoftware.os.amza.storage.binary;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.TransactionEntry;
import java.util.Map;

public class BinaryRowMarshaller<K, V> implements RowMarshaller<K, V, byte[]> {

    private final TableName<K, V> tableName;

    public BinaryRowMarshaller(TableName<K, V> tableName) {
        this.tableName = tableName;
    }

    @Override
    public TableName<K, V> getTableName() {
        return tableName;
    }

    @Override
    public byte[] toRow(long orderId, Map.Entry<K, TimestampedValue<V>> e) throws JsonProcessingException {
        String transaction = Long.toString(orderId);
        String key = null; //mapper.writeValueAsString(e.getKey());
        String timestamp = Long.toString(e.getValue().getTimestamp());
        String tombstone = Boolean.toString(e.getValue().getTombstoned());
        String value = null; //mapper.writeValueAsString(e.getValue().getValue());

        StringBuilder sb = new StringBuilder();
        sb.append("transaction.").append(transaction.length()).append('=').append(transaction).append(',');
        sb.append("key.").append(key.length()).append('=').append(key).append(',');
        sb.append("tombstone.").append(tombstone.length()).append('=').append(tombstone).append(',');
        sb.append("timestamp.").append(timestamp.length()).append('=').append(timestamp).append(',');
        sb.append("value.").append(value.length()).append('=').append(value);
        return null; // todo sb.toString();
    }

    @Override
    public TransactionEntry<K, V> fromRow(byte[] row) throws Exception {
//        StringAndOffest stringAndOffest = value(row, 0);
//        long orderId = Long.parseLong(stringAndOffest.getString());
//        stringAndOffest = value(row, stringAndOffest.getOffset());
//        K key = mapper.readValue(stringAndOffest.getString(), partitionName.getKeyClass());
//        stringAndOffest = value(row, stringAndOffest.getOffset());
//        boolean tombstone = Boolean.parseBoolean(stringAndOffest.getString());
//        stringAndOffest = value(row, stringAndOffest.getOffset());
//        long timestamp = Long.parseLong(stringAndOffest.getString());
//        stringAndOffest = value(row, stringAndOffest.getOffset());
//        V value = mapper.readValue(stringAndOffest.getString(), partitionName.getValueClass());
//        return new BasicTransactionEntry<>(orderId, key, new TimestampedValue<>(value, timestamp, tombstone));
        return null; // TODO
    }
}