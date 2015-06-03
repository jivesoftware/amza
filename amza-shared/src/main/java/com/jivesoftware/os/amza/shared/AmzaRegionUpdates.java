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
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class AmzaRegionUpdates implements Commitable<WALValue> {

    private final ConcurrentSkipListMap<WALKey, WALValue> changes;

    public AmzaRegionUpdates() {
        this.changes = new ConcurrentSkipListMap<>();
    }

    public void setAll(Iterable<Entry<byte[], byte[]>> updates, long timestampId) throws Exception {
        for (Entry<byte[], byte[]> update : updates) {
            set(update.getKey(), update.getValue(), timestampId);
        }
    }

    public boolean set(byte[] key, byte[] value, long timestampId) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        changes.merge(new WALKey(key), new WALValue(value, timestampId, false), (existing, provided) -> {
            if (provided.getTimestampId() >= existing.getTimestampId()) {
                return provided;
            } else {
                return existing;
            }
        });
        return true;
    }

    public void removeAll(Iterable<byte[]> keys, long timestampId) throws Exception {
        for (byte[] key : keys) {
            remove(key, timestampId);
        }
    }

    public void remove(byte[] key, long timestamp) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        changes.merge(new WALKey(key), new WALValue(null, timestamp, true), (existing, provided) -> {
            if (provided.getTimestampId() >= existing.getTimestampId()) {
                return provided;
            } else {
                return existing;
            }
        });
    }

    @Override
    public void commitable(Highwaters highwaters, Scan<WALValue> scan) throws Exception {
        for (Entry<WALKey, WALValue> e : changes.entrySet()) {
            WALValue value = e.getValue();
            if (!scan.row(value.getTimestampId(), e.getKey(), value)) {
                return;
            }
        }
    }

}
