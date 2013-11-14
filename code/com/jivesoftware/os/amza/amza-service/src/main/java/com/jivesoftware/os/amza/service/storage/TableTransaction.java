package com.jivesoftware.os.amza.service.storage;

public class TableTransaction<K, V> {

    private final TableStore<K, V> sortedMapStore;
    private final IsolatedChanges<K, V> changesMap;
    private int changedCount = 0;

    TableTransaction(TableStore<K, V> sortedMapStore,
            IsolatedChanges<K, V> updateableMap) {
        this.sortedMapStore = sortedMapStore;
        this.changesMap = updateableMap;
    }

    public K add(K key, V value) throws Exception {
        if (changesMap.put(key, value)) {
            changedCount++;
        }
        return key;
    }

    public boolean remove(K key) throws Exception {
        if (changesMap.containsKey(key)) {
            V got = changesMap.getValue(key);
            if (got != null) {
                if (changesMap.remove(key)) {
                    changedCount++;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public void commit() throws Exception {
        if (changedCount > 0) {
            sortedMapStore.commit(changesMap.getChangesMap());
        }
    }
}