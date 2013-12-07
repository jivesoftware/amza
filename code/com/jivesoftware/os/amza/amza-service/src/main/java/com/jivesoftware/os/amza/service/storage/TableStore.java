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

import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowScanable;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.RowsStorage;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableStore implements RowScanable {

    private final RowsStorage rowsStorage;
    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final RowChanges rowChanges;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public TableStore(RowsStorage rowsStorage, RowChanges rowChanges) {
        this.rowsStorage = rowsStorage;
        this.rowChanges = rowChanges;
    }

    public void load() throws Exception {
        if (!loaded.get()) {
            try {
                readWriteLock.writeLock().lock();
                if (loaded.compareAndSet(false, true)) {
                    try {

                        rowsStorage.load();
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
        rowsStorage.compactTombestone(ifOlderThanNMillis);
    }

    public byte[] get(RowIndexKey key) throws Exception {
        RowIndexValue got;
        try {
            readWriteLock.readLock().lock();
            got = rowsStorage.get(key);
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

    public RowIndexValue getTimestampedValue(RowIndexKey key) throws Exception {
        try {
            readWriteLock.readLock().lock();
            return rowsStorage.get(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public boolean containsKey(RowIndexKey key) throws Exception {
        try {
            readWriteLock.readLock().lock();
            return rowsStorage.containsKey(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public <E extends Exception> void rowScan(RowScan<E> stream) throws E {
        rowsStorage.rowScan(stream);
    }

    public void takeRowUpdatesSince(long transactionId, RowScan rowUpdates) throws Exception {
        rowsStorage.takeRowUpdatesSince(transactionId, rowUpdates);
    }

    public RowStoreUpdates startTransaction(long timestamp) throws Exception {
        return new RowStoreUpdates(this, new RowsStorageUpdates(rowsStorage, timestamp));
    }

    public void commit(RowScanable changes) throws Exception {
        RowsChanged updateMap = rowsStorage.update(changes);
        if (!updateMap.isEmpty()) {
            try {
                readWriteLock.writeLock().lock();
                NavigableMap<RowIndexKey, RowIndexValue> apply = updateMap.getApply();
                for (Map.Entry<RowIndexKey, RowIndexValue> entry : apply.entrySet()) {
                    RowIndexKey k = entry.getKey();
                    RowIndexValue timestampedValue = entry.getValue();
                    RowIndexValue got = rowsStorage.get(k);
                    if (got == null) {
                        rowsStorage.put(k, timestampedValue);
                    } else if (got.getTimestamp() < timestampedValue.getTimestamp()) {
                        rowsStorage.put(k, timestampedValue);
                    }
                }
                NavigableMap<RowIndexKey, RowIndexValue> remove = updateMap.getRemove();
                for (Map.Entry<RowIndexKey, RowIndexValue> entry : remove.entrySet()) {
                    RowIndexKey k = entry.getKey();
                    RowIndexValue timestampedValue = entry.getValue();
                    RowIndexValue got = rowsStorage.get(k);
                    if (got != null && got.getTimestamp() < timestampedValue.getTimestamp()) {
                        rowsStorage.remove(k);
                    }
                }

            } finally {
                readWriteLock.writeLock().unlock();
            }
            if (rowChanges != null) {
                rowChanges.changes(updateMap);
            }
        }
    }

    public void clear() throws Exception {
        try {
            readWriteLock.writeLock().lock();
            rowsStorage.clear();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
