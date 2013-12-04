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

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.NavigableMap;

public class MemoryTableIndex implements TableIndex, Serializable {

    private final NavigableMap<TableIndexKey, TimestampedValue> index;

    public MemoryTableIndex(NavigableMap<TableIndexKey, TimestampedValue> index) {
        this.index = index;
    }

    @Override
    public TimestampedValue put(TableIndexKey key, TimestampedValue value) {
        return index.put(key, value);
    }

    @Override
    public TimestampedValue get(TableIndexKey key) {
        return index.get(key);
    }

    @Override
    public boolean containsKey(TableIndexKey key) {
        return index.containsKey(key);
    }

    @Override
    public TimestampedValue remove(TableIndexKey key) {
        return index.remove(key);
    }

    @Override
    public <E extends Throwable> void entrySet(EntryStream<E> entryStream) {
        for (Entry<TableIndexKey, TimestampedValue> e : index.entrySet()) {
            try {
                if (!entryStream.stream(e.getKey(), e.getValue())) {
                    break;
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Error while streaming entry set.", ex);
            }
        }
    }

    @Override
    public void clear() {
        index.clear();
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public void flush() {
    }
}
