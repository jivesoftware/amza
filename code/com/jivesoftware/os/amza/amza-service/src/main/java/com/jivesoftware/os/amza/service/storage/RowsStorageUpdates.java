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

import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowScanable;
import com.jivesoftware.os.amza.shared.RowsStorage;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Not thread safe. Each Thread should get their own ReadThroughChangeSet.
 *
 */
public class RowsStorageUpdates implements RowScanable {

    private final RowsStorage rowsStorage;
    private final ConcurrentSkipListMap<RowIndexKey, RowIndexValue> changes;
    private final long timestamp;

    RowsStorageUpdates(RowsStorage rowsStorage, long timestamp) {
        this.rowsStorage = rowsStorage;
        this.timestamp = timestamp;
        this.changes = new ConcurrentSkipListMap<>();
    }

    @Override
    public <E extends Exception> void rowScan(RowScan<E> rowStream) throws E {
        for (Entry<RowIndexKey, RowIndexValue> e : changes.entrySet()) {
            if (!rowStream.row(timestamp, e.getKey(), e.getValue())) {
                return;
            }
        }
    }

    public boolean containsKey(RowIndexKey key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        return rowsStorage.containsKey(key);
    }

    public byte[] getValue(RowIndexKey key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        RowIndexValue got = rowsStorage.get(key);
        if (got == null || got.getTombstoned()) {
            return null;
        }
        return got.getValue();
    }

    public RowIndexValue getTimestampedValue(RowIndexKey key) {
        if (key == null) {
            return null;
        }
        return rowsStorage.get(key);
    }

    public boolean put(RowIndexKey key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        long putTimestamp = timestamp + 1;
        RowIndexValue update = new RowIndexValue(value, putTimestamp, false);
        RowIndexValue current = rowsStorage.get(key);
        if (current == null || current.getTimestamp() < update.getTimestamp()) {
            changes.put(key, update);
            return true;
        }
        return false;
    }

    public boolean remove(RowIndexKey key) throws IOException {
        if (key == null) {
            return false;
        }
        long removeTimestamp = timestamp;
        RowIndexValue current = rowsStorage.get(key);
        byte[] value = (current != null) ? current.getValue() : null;
        RowIndexValue update = new RowIndexValue(value, removeTimestamp, true);
        if (current == null || current.getTimestamp() < update.getTimestamp()) {
            changes.put(key, update);
            return true;
        }
        return false;
    }
}
