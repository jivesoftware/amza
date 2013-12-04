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

import com.jivesoftware.os.amza.shared.BinaryTimestampedValue;
import com.jivesoftware.os.amza.shared.ImmutableTableIndex;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteTableStore {

    private final TableStorage tableStorage;
    private final AtomicReference<TableIndex> readMap;
    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final TableStateChanges tableStateChanges;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public ReadWriteTableStore(TableStorage tableStorage, TableStateChanges tableStateChanges) {
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

    public byte[] get(TableIndexKey key) throws Exception {
        BinaryTimestampedValue got;
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

    public BinaryTimestampedValue getTimestampedValue(TableIndexKey key) throws Exception {
        try {
            readWriteLock.readLock().lock();
            return readMap.get().get(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public boolean containsKey(TableIndexKey key) throws Exception {
        try {
            readWriteLock.readLock().lock();
            return readMap.get().containsKey(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public TableIndex getImmutableCopy() {
        return new ImmutableTableIndex(readMap.get());
    }

    public void getMutatedRowsSince(long transactionId, TransactionSetStream transactionSetStream) throws Exception {
        tableStorage.rowMutationSince(transactionId, transactionSetStream);
    }

    public ReadThroughChangeSet getReadThroughChangeSet(long timestamp) throws Exception {
        return new ReadThroughChangeSet(readMap.get(), timestamp);
    }

    public void commit(TableIndex changes) throws Exception {
        TableIndex currentReadMap = readMap.get();
        TableDelta updateMap = tableStorage.update(changes, currentReadMap);
        try {
            readWriteLock.writeLock().lock();
            NavigableMap<TableIndexKey, BinaryTimestampedValue> apply = updateMap.getApply();
            for (Map.Entry<TableIndexKey, BinaryTimestampedValue> entry : apply.entrySet()) {
                TableIndexKey k = entry.getKey();
                BinaryTimestampedValue timestampedValue = entry.getValue();
                BinaryTimestampedValue got = currentReadMap.get(k);
                if (got == null) {
                    currentReadMap.put(k, timestampedValue);
                } else if (got.getTimestamp() < timestampedValue.getTimestamp()) {
                    currentReadMap.put(k, timestampedValue);
                }
            }
            NavigableMap<TableIndexKey, BinaryTimestampedValue> remove = updateMap.getRemove();
            for (Map.Entry<TableIndexKey, BinaryTimestampedValue> entry : remove.entrySet()) {
                TableIndexKey k = entry.getKey();
                BinaryTimestampedValue timestampedValue = entry.getValue();
                BinaryTimestampedValue got = currentReadMap.get(k);
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
