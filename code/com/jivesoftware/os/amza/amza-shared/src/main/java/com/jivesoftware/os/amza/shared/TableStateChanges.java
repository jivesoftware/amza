package com.jivesoftware.os.amza.shared;

public interface TableStateChanges<K, V> {

    void changes(TableName<K, V> tableName, TableDelta<K, V> changes) throws Exception;
}