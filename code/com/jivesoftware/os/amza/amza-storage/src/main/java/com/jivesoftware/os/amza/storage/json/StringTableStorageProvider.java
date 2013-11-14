package com.jivesoftware.os.amza.storage.json;

import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TableStorageProvider;
import com.jivesoftware.os.amza.storage.FileBackedTableStorage;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.RowMarshallerProvider;
import com.jivesoftware.os.amza.storage.RowTableFile;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.File;

public class StringTableStorageProvider implements TableStorageProvider {

    private final OrderIdProvider orderIdProvider;
    private final RowMarshallerProvider rowMarshallerProvider;

    public StringTableStorageProvider(OrderIdProvider orderIdProvider, RowMarshallerProvider rowMarshallerProvider) {
        this.orderIdProvider = orderIdProvider;
        this.rowMarshallerProvider = rowMarshallerProvider;
    }

    @Override
    public <K, V> TableStorage<K, V> createTableStorage(File workingDirectory, String tableDomain, TableName<K, V> tableName) {

        File file = new File(workingDirectory, tableDomain + File.separator + tableName.getTableName() + ".kvt");
        StringRowReader reader = new StringRowReader(file);
        StringRowWriter writer = new StringRowWriter(file);

        RowMarshaller<K, V, String> rowMarshaller = rowMarshallerProvider.getRowMarshaller(tableName);
        RowTableFile<K, V, String> rowTableFile = new RowTableFile<>(orderIdProvider, rowMarshaller, reader, writer);
        return new FileBackedTableStorage(rowTableFile);
    }
}