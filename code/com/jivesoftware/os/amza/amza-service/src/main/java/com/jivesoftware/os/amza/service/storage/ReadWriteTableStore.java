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
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteTableStore<K, V> {

    private final TableStorage<K, V> tableStorage;
    private final AtomicReference<NavigableMap<K, TimestampedValue<V>>> readMap;
    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final TableStateChanges tableStateChanges;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ReadWriteTableStore(TableStorage<K, V> tableStorage, TableStateChanges tableStateChanges) {
        this.tableStorage = tableStorage;
        this.readMap = new AtomicReference<>(null);
        this.tableStateChanges = tableStateChanges;
    }

    public TableName getTableName() {
        return tableStorage.getTableName();
    }


    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        tableStorage.compactTombestone(ifOlderThanNMillis);
    }

    public V get(K key) throws Exception {
        load();
        TimestampedValue<V> got;
        try {
            lock.readLock().lock();
            got = readMap.get().get(key);
        } finally {
            lock.readLock().unlock();
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
        load();
        try {
            lock.readLock().lock();
            return readMap.get().get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean containsKey(K key) throws Exception {
        load();
        try {
            lock.readLock().lock();
            return readMap.get().containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public NavigableMap<K, TimestampedValue<V>> getImmutableCopy() throws Exception {
        load();
        return Maps.unmodifiableNavigableMap(readMap.get());
    }

    public void getMutatedRowsSince(long transactionId, TransactionSetStream<K, V> transactionSetStream) throws Exception {
        tableStorage.rowMutationSince(transactionId, transactionSetStream);
    }

    public ReadThroughChangeSet<K, V> getReadThroughChangeSet(long timestamp) throws Exception {
        load();
        return new ReadThroughChangeSet<>(readMap.get(), timestamp);
    }

    synchronized public void commit(NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        load();
        NavigableMap<K, TimestampedValue<V>> currentReadMap = readMap.get();
        TableDelta<K, V> updateMap = tableStorage.update(changes, currentReadMap);
        try {
            lock.writeLock().lock();
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
            lock.writeLock().unlock();
        }
        if (tableStateChanges != null) {
            tableStateChanges.changes(tableStorage.getTableName(), updateMap);
        }
    }

    synchronized public void clear() throws Exception {
        ConcurrentNavigableMap<K, TimestampedValue<V>> empty = new ConcurrentSkipListMap<>();
        readMap.set(empty);
        tableStorage.clear();
    }

    synchronized public void load() throws Exception {
        if (!loaded.get()) {
            SortedMap<K, TimestampedValue<V>> sortedMap = tableStorage.load();
            readMap.set(new ConcurrentSkipListMap<>(sortedMap));
            loaded.set(true);
        }
    }
}