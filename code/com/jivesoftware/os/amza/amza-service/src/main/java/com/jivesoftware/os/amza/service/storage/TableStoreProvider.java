package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TableStorageProvider;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TableStoreProvider {

    private final File workingDirectory;
    private final String storeName;
    private final TableStorageProvider  tableStorageProvider;
    private final TableStateChanges tableStateChanges;
    private final ConcurrentHashMap<TableName, TableStore<?, ?>> tableStores = new ConcurrentHashMap<>();

    public TableStoreProvider(File workingDirectory,
            String storeName,
            TableStorageProvider tableStorageProvider,
            TableStateChanges tableStateChanges) {
        this.workingDirectory = workingDirectory;
        this.storeName = storeName;
        this.tableStorageProvider = tableStorageProvider;
        this.tableStateChanges = tableStateChanges;
    }

    public String getName() {
        return storeName;
    }

    synchronized public <K, V> TableStore<K, V> getTableStore(TableName<K, V> tableName) throws Exception {
        TableStore<K, V> tableStore = (TableStore<K, V>) tableStores.get(tableName);
        if (tableStore == null) {
            TableStorage<K, V> tableStorage = tableStorageProvider.createTableStorage(workingDirectory, storeName, tableName);
            ReadWriteTableStore<K, V> readWriteTableStore = new ReadWriteTableStore<>(tableStorage, tableStateChanges);
            tableStore = new TableStore(readWriteTableStore);
            tableStores.put(tableName, tableStore);
        }
        return tableStore;
    }

    public Set<Map.Entry<TableName, TableStore<?, ?>>> getTableStores() {
        return tableStores.entrySet();
    }
}