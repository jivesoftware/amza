package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.BasicTimestampedValue;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.storage.BasicTransactionEntry;
import com.jivesoftware.os.amza.storage.FstMarshaller;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.TransactionEntry;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.util.Map;

public class BinaryRowMarshaller<K, V> implements RowMarshaller<K, V, byte[]> {

    private static final FstMarshaller FST_MARSHALLER = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());

    static {
        FST_MARSHALLER.registerSerializer(BinaryRow.class, new FSTBinaryRowMarshaller());
    }
    private final TableName<K, V> tableName;

    public BinaryRowMarshaller(TableName<K, V> tableName) {
        this.tableName = tableName;
    }

    @Override
    public TableName<K, V> getTableName() {
        return tableName;
    }

    @Override
    public byte[] toRow(long orderId, Map.Entry<K, TimestampedValue<V>> e) throws Exception {
        return FST_MARSHALLER.serialize(new BinaryRow(orderId,
                FST_MARSHALLER.serialize(e.getKey()),
                e.getValue().getTimestamp(),
                e.getValue().getTombstoned(),
                FST_MARSHALLER.serialize(e.getValue().getValue())));
    }

    @Override
    public TransactionEntry<K, V> fromRow(byte[] row) throws Exception {
        BinaryRow binaryRow = FST_MARSHALLER.deserialize(row, BinaryRow.class);
        return new BasicTransactionEntry<>(binaryRow.transaction,
                FST_MARSHALLER.deserialize(binaryRow.key, tableName.getKeyClass()),
                new BasicTimestampedValue(
                        FST_MARSHALLER.deserialize(binaryRow.value, tableName.getValueClass()),
                        binaryRow.timestamp,
                        binaryRow.tombstone)
        );
    }

}
