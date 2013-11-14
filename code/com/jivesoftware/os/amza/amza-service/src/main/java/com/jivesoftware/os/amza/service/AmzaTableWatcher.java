package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import java.util.concurrent.ConcurrentHashMap;

public class AmzaTableWatcher implements TableStateChanges<Object, Object> {

    private final TableStateChanges tableStateChanges;
    private final ConcurrentHashMap<TableName<?, ?>, TableStateChanges> watchers = new ConcurrentHashMap<>();

    public AmzaTableWatcher(TableStateChanges tableStateChanges) {
        this.tableStateChanges = tableStateChanges;
    }

    @Override
    public void changes(TableName<Object, Object> tableName, TableDelta<Object, Object> changes) throws Exception {
        tableStateChanges.changes(tableName, changes);
        TableStateChanges watcher = watchers.get(tableName);
        if (watcher != null) {
            watcher.changes(tableName, changes);
        }
    }

    public <K, V> void watch(TableName<K, V> tableName, TableStateChanges tableStateChanges) throws Exception {
        watchers.put(tableName, tableStateChanges);
    }

    public <K, V> TableStateChanges unwatch(TableName<K, V> tableName) throws Exception {
        return watchers.remove(tableName);
    }
}