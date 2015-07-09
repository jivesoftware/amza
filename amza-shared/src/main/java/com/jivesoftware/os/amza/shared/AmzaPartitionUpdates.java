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
package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.TxKeyValueStream;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class AmzaPartitionUpdates implements Commitable {

    private final ConcurrentSkipListMap<WALKey, WALValue> changes;

    public AmzaPartitionUpdates() {
        this.changes = new ConcurrentSkipListMap<>();
    }

    public AmzaPartitionUpdates setAll(Iterable<Entry<WALKey, byte[]>> updates) throws Exception {
        setAll(updates, -1);
        return this;
    }

    public AmzaPartitionUpdates setAll(Iterable<Entry<WALKey, byte[]>> updates, long timestampId) throws Exception {
        for (Entry<WALKey, byte[]> update : updates) {
            set(update.getKey(), update.getValue(), timestampId);
        }
        return this;
    }

    public AmzaPartitionUpdates set(WALKey key, byte[] value) throws Exception {
        return set(key, value, -1);
    }

    public AmzaPartitionUpdates set(WALKey key, byte[] value, long timestampId) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        changes.merge(key, new WALValue(value, timestampId, false), (existing, provided) -> {
            if (provided.getTimestampId() >= existing.getTimestampId()) {
                return provided;
            } else {
                return existing;
            }
        });
        return this;
    }

    public AmzaPartitionUpdates removeAll(Iterable<WALKey> keys, long timestampId) throws Exception {
        for (WALKey key : keys) {
            remove(key, timestampId);
        }
        return this;
    }

    public AmzaPartitionUpdates remove(WALKey key) throws Exception {
        return remove(key, -1);
    }

    public AmzaPartitionUpdates remove(WALKey key, long timestamp) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        changes.merge(key, new WALValue(null, timestamp, true), (existing, provided) -> {
            if (provided.getTimestampId() >= existing.getTimestampId()) {
                return provided;
            } else {
                return existing;
            }
        });
        return this;
    }

    @Override
    public boolean commitable(Highwaters highwaters, TxKeyValueStream txKeyValueStream) throws Exception {
        for (Entry<WALKey, WALValue> e : changes.entrySet()) {
            WALValue value = e.getValue();
            if (!txKeyValueStream.row(-1, e.getKey().getKey(), value.getValue(), value.getTimestampId(), value.getTombstoned())) {
                return false;
            }
        }
        return true;
    }

}
