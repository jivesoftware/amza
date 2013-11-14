package com.jivesoftware.os.amza.shared;

import java.io.File;

public interface TableStorageProvider {

    <K, V> TableStorage<K, V> createTableStorage(File workingDirectory, String storeName, TableName<K, V> tableName);
}