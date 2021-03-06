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
package com.jivesoftware.os.amza.service;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.CommitKeyValueStream;
import com.jivesoftware.os.amza.api.wal.WALValue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AmzaPartitionUpdates implements ClientUpdates {

    private final ConcurrentSkipListMap<byte[], WALValue> changes = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
    private final AtomicInteger approximateSize = new AtomicInteger(); // Because changes.size() walks the entire collection to compute size :(

    public AmzaPartitionUpdates set(byte[] key, byte[] value, long timestampId) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        update(key, value, timestampId, false);
        return this;
    }

    public AmzaPartitionUpdates remove(byte[] key, long timestampId) throws Exception {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        update(key, null, timestampId, true);
        return this;
    }

    private void update(byte[] key, byte[] value, long timestampId, boolean tombstone) {
        changes.compute(key, (byte[] k, WALValue existing) -> {
            if (existing == null) {
                approximateSize.incrementAndGet();
                return new WALValue(null, value, timestampId, tombstone, -1);
            } else if (timestampId >= existing.getTimestampId()) {
                return new WALValue(null, value, timestampId, tombstone, -1);
            } else {
                return existing;
            }
        });
    }

    public void reset() {
        approximateSize.set(0);
        changes.clear();
    }

    public int size() {
        return approximateSize.get();
    }

    @Override
    public boolean updates(CommitKeyValueStream commitKeyValueStream) throws Exception {
        for (Entry<byte[], WALValue> e : changes.entrySet()) {
            WALValue value = e.getValue();
            if (!commitKeyValueStream.commit(e.getKey(), value.getValue(), value.getTimestampId(), value.getTombstoned())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "AmzaPartitionUpdates{"
            + "changes=" + changes
            + '}';
    }
}
