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
package com.jivesoftware.os.amza.service.storage;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteTableStore<K, V> {

    private final TableStorage<K, V> tableStorage;
    private final AtomicReference<TableIndex<K, V>> readMap;
    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final TableStateChanges<K, V> tableStateChanges;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public ReadWriteTableStore(TableStorage<K, V> tableStorage, TableStateChanges<K, V> tableStateChanges) {
        this.tableStorage = tableStorage;
        this.readMap = new AtomicReference<>(null);
        this.tableStateChanges = tableStateChanges;
    }

    public TableName getTableName() {
        return tableStorage.getTableName();
    }

    public void load() throws Exception {
        if (!loaded.get()) {
            try {
                readWriteLock.writeLock().lock();
                if (loaded.compareAndSet(false, true)) {
                    try {
                        readMap.set(tableStorage.load());
                    } catch (Exception x) {
                        throw x;
                    } finally {
                        loaded.set(false);
                    }
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        tableStorage.compactTombestone(ifOlderThanNMillis);
    }

    public V get(K key) throws Exception {
        TimestampedValue<V> got;
        try {
            readWriteLock.readLock().lock();
            got = readMap.get().get(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
        if (got == null) {
            return null;
        }
        if (got.getTombstoned()) {
            return null;
        }
        return got.getValue();
    }

    public TimestampedValue<V> getTimestampedValue(K key) throws Exception {
        try {
            readWriteLock.readLock().lock();
            return readMap.get().get(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public boolean containsKey(K key) throws Exception {
        try {
            readWriteLock.readLock().lock();
            return readMap.get().containsKey(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public NavigableMap<K, TimestampedValue<V>> getImmutableCopy() throws Exception {
        return Maps.unmodifiableNavigableMap(readMap.get());
    }

    public void getMutatedRowsSince(long transactionId, TransactionSetStream<K, V> transactionSetStream) throws Exception {
        tableStorage.rowMutationSince(transactionId, transactionSetStream);
    }

    public ReadThroughChangeSet<K, V> getReadThroughChangeSet(long timestamp) throws Exception {
        return new ReadThroughChangeSet<>(readMap.get(), timestamp);
    }

    public void commit(NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        NavigableMap<K, TimestampedValue<V>> currentReadMap = readMap.get();
        TableDelta<K, V> updateMap = tableStorage.update(changes, currentReadMap);
        try {
            readWriteLock.writeLock().lock();
            NavigableMap<K, TimestampedValue<V>> apply = updateMap.getApply();
            for (Map.Entry<K, TimestampedValue<V>> entry : apply.entrySet()) {
                K k = entry.getKey();
                TimestampedValue<V> timestampedValue = entry.getValue();
                TimestampedValue<V> got = currentReadMap.get(k);
                if (got == null) {
                    currentReadMap.put(k, timestampedValue);
                } else if (got.getTimestamp() < timestampedValue.getTimestamp()) {
                    currentReadMap.put(k, timestampedValue);
                }
            }
            NavigableMap<K, TimestampedValue<V>> remove = updateMap.getRemove();
            for (Map.Entry<K, TimestampedValue<V>> entry : remove.entrySet()) {
                K k = entry.getKey();
                TimestampedValue<V> timestampedValue = entry.getValue();
                TimestampedValue<V> got = currentReadMap.get(k);
                if (got != null && got.getTimestamp() < timestampedValue.getTimestamp()) {
                    currentReadMap.remove(k);
                }
            }

        } finally {
            readWriteLock.writeLock().unlock();
        }
        if (tableStateChanges != null) {
            tableStateChanges.changes(tableStorage.getTableName(), updateMap);
        }
    }

    public void clear() throws Exception {
        try {
            readWriteLock.writeLock().lock();
            readMap.get().clear();
            tableStorage.clear();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}