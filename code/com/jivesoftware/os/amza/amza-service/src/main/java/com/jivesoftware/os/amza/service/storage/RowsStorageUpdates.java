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

import com.jivesoftware.os.amza.service.replication.RegionStripe;
import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Not thread safe. Each Thread should get their own ReadThroughChangeSet.
 */
public class RowsStorageUpdates implements RangeScannable<WALValue> {

    private final RegionName regionName;
    private final RegionStripe regionStripe;
    private final ConcurrentSkipListMap<WALKey, WALValue> changes;
    private final long timestamp;

    public RowsStorageUpdates(RegionName regionName, RegionStripe regionStripe, long timestamp) {
        this.regionName = regionName;
        this.regionStripe = regionStripe;
        this.timestamp = timestamp;
        this.changes = new ConcurrentSkipListMap<>();
    }

    @Override
    public void rowScan(Scan<WALValue> scan) throws Exception {
        for (Entry<WALKey, WALValue> e : changes.entrySet()) {
            if (!scan.row(timestamp, e.getKey(), e.getValue())) {
                return;
            }
        }
    }

    @Override
    public void rangeScan(WALKey from, WALKey to, Scan<WALValue> scan) throws Exception {
        for (Entry<WALKey, WALValue> e : changes.subMap(from, to).entrySet()) {
            if (!scan.row(timestamp, e.getKey(), e.getValue())) {
                return;
            }
        }
    }

    public boolean containsKey(WALKey key) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        return regionStripe.containsKey(regionName, key);
    }

    public byte[] getValue(WALKey key) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        WALValue got = regionStripe.get(regionName, key);
        if (got == null || got.getTombstoned()) {
            return null;
        }
        return got.getValue();
    }

    public WALValue getTimestampedValue(WALKey key) throws Exception {
        if (key == null) {
            return null;
        }
        return regionStripe.get(regionName, key);
    }

    public boolean put(WALKey key, byte[] value) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        WALValue update = new WALValue(value, timestamp, false);
        changes.put(key, update);
        return true;
    }

    public boolean remove(WALKey key) throws Exception {
        if (key == null) {
            return false;
        }
        WALValue current = regionStripe.get(regionName, key);
        byte[] value = (current != null) ? current.getValue() : null;
        WALValue update = new WALValue(value, timestamp, true);
        if (current == null || current.getTimestampId() < update.getTimestampId()) {
            changes.put(key, update);
            return true;
        }
        return false;
    }

}
