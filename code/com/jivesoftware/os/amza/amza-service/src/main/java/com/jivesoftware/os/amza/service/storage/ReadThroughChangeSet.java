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

import com.jivesoftware.os.amza.shared.BasicTimestampedValue;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Not thread safe. Each Thread should get their own ReadThroughChangeSet.
 *
 */
public class ReadThroughChangeSet {

    private final TableIndex writeMap;
    private final ConcurrentSkipListMap<TableIndexKey, TimestampedValue> changes;
    private final long timestamp;

    ReadThroughChangeSet(TableIndex writeMap, long timestamp) {
        this.writeMap = writeMap;
        this.timestamp = timestamp;
        this.changes = new ConcurrentSkipListMap<>();
    }

    ConcurrentSkipListMap<TableIndexKey, TimestampedValue> getChangesMap() {
        return changes;
    }

    public boolean containsKey(TableIndexKey key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        return writeMap.containsKey(key);
    }

    public byte[] getValue(TableIndexKey key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        TimestampedValue got = writeMap.get(key);
        if (got == null || got.getTombstoned()) {
            return null;
        }
        return got.getValue();
    }

    public TimestampedValue getTimestampedValue(TableIndexKey key) {
        if (key == null) {
            return null;
        }
        return writeMap.get(key);
    }

    public boolean put(TableIndexKey key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        long putTimestamp = timestamp + 1;
        TimestampedValue update = new BasicTimestampedValue(value, putTimestamp, false);
        TimestampedValue current = writeMap.get(key);
        if (current == null || current.getTimestamp() < update.getTimestamp()) {
            changes.put(key, update);
            return true;
        }
        return false;
    }

    public boolean remove(TableIndexKey key) throws IOException {
        if (key == null) {
            return false;
        }
        long removeTimestamp = timestamp;
        TimestampedValue current = writeMap.get(key);
        byte[] value = (current != null) ? current.getValue() : null;
        TimestampedValue update = new BasicTimestampedValue(value, removeTimestamp, true);
        if (current == null || current.getTimestamp() < update.getTimestamp()) {
            changes.put(key, update);
            return true;
        }
        return false;
    }
}
