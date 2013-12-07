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
package com.jivesoftware.os.amza.storage.index;

import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.Flusher;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowsIndex;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.ValueStorage;
import com.jivesoftware.os.amza.storage.chunks.UIO;
import java.util.Map.Entry;
import java.util.NavigableMap;

public class NavigableMapRowsIndex implements RowsIndex {

    private final NavigableMap<RowIndexKey, RowIndexValue> index;
    private final Flusher flusher;
    private final ValueStorage valueStorage;

    public NavigableMapRowsIndex(NavigableMap<RowIndexKey, RowIndexValue> index, Flusher flusher, ValueStorage valueStorage) throws Exception {
        this.index = index;
        this.flusher = flusher;
        this.valueStorage = valueStorage;
    }

    @Override
    public void commit() {
        flusher.flush();
    }

    @Override
    public void compact() {
        //flusher.compact(); // TODO?
    }

    @Override
    public <E extends Exception> void rowScan(RowScan<E> entryStream) {

        for (Entry<RowIndexKey, RowIndexValue> e : index.entrySet()) {
            RowIndexKey key = e.getKey();
            RowIndexValue value = e.getValue();
            LazyLoadingTimestampValue lazyLoadingTimestampValue = new LazyLoadingTimestampValue(value.getValue(),
                    value.getTimestamp(),
                    value.getTombstoned());
            try {
                if (!entryStream.row(-1, key, lazyLoadingTimestampValue)) {
                    break;
                }
            } catch (Throwable t) {
                throw new RuntimeException("Failed while streaming entry set.", t);
            }
        }
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public boolean containsKey(RowIndexKey key) {
        return index.containsKey(key);
    }

    @Override
    public RowIndexValue get(RowIndexKey key) {
        RowIndexValue got = index.get(key);
        if (got == null) {
            return null;
        }
        return new LazyLoadingTimestampValue(got.getValue(), got.getTimestamp(), got.getTombstoned());
    }

    @Override
    public RowIndexValue put(RowIndexKey key,
            RowIndexValue value) {
        byte[] chunkId;
        try {
            chunkId = valueStorage.put(value.getValue());
        } catch (Exception x) {
            throw new RuntimeException("Failed to save value to chuck filer. " + value.getValue().getClass(), x);
        }

        RowIndexValue basicTimestampedValue = new RowIndexValue(chunkId, value.getTimestamp(), value.getTombstoned());
        RowIndexValue had = index.put(key, basicTimestampedValue);
        if (had == null) {
            return null;
        }
        return new LazyLoadingTimestampValue(had.getValue(), had.getTimestamp(), had.getTombstoned());
    }

    @Override
    public RowIndexValue remove(RowIndexKey key) {
        RowIndexValue removed = index.remove(key);
        if (removed == null) {
            return null;
        }
        // TODO should we load the value and delete it from the chunk filer?
        return new LazyLoadingTimestampValue(removed.getValue(), removed.getTimestamp(), removed.getTombstoned());
    }

    @Override
    public void clear() {
        // TODO should we clean out the chunkfiler
        valueStorage.clear();
        index.clear();
    }

    public class LazyLoadingTimestampValue extends RowIndexValue {

        public LazyLoadingTimestampValue(byte[] value,
                long timestamp,
                boolean tombstoned) {
            super(value, timestamp, tombstoned);
        }

        @Override
        public byte[] getValue() {
            try {
                return valueStorage.get(super.getValue());
            } catch (Exception x) {
                throw new RuntimeException("Unable to read value from chunkFiler for chunkId:" + UIO.bytesLong(super.getValue()), x);
            }
        }
    }
}
