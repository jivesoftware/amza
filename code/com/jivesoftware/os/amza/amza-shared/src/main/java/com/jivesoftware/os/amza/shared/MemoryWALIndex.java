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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryWALIndex implements WALIndex, Serializable {

    private final NavigableMap<WALKey, WALValue> index;

    public MemoryWALIndex() {
        this(new ConcurrentSkipListMap<WALKey, WALValue>());
    }

    public MemoryWALIndex(NavigableMap<WALKey, WALValue> index) {
        this.index = index;
    }

    @Override
    public void commit() {

    }

    @Override
    public void compact() {

    }

    @Override
    public <E extends Exception> void rowScan(WALScan<E> rowScan) throws Exception {
        for (Entry<WALKey, WALValue> e : index.entrySet()) {
            WALKey key = e.getKey();
            WALValue value = e.getValue();
            if (!rowScan.row(-1, key, value)) {
                break;
            }
        }
    }

    @Override
    public <E extends Exception> void rangeScan(WALKey from, WALKey to, WALScan<E> rowScan) throws E {
        for (Entry<WALKey, WALValue> e : index.subMap(from, to).entrySet()) {
            WALKey key = e.getKey();
            WALValue value = e.getValue();
            if (!rowScan.row(-1, key, value)) {
                break;
            }
        }
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public List<Boolean> containsKey(List<WALKey> keys) {
        List<Boolean> contains = new ArrayList<>(keys.size());
        for (WALKey key : keys) {
            contains.add(index.containsKey(key));
        }
        return contains;
    }

    @Override
    public List<WALValue> get(List<WALKey> keys) {
        List<WALValue> gots = new ArrayList<>(keys.size());
        for (WALKey key : keys) {
            gots.add(index.get(key));
        }
        return gots;
    }

    @Override
    public void put(Collection<? extends Map.Entry<WALKey, WALValue>> entrys) {
        for (Map.Entry<WALKey, WALValue> entry : entrys) {
            index.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(Collection<WALKey> keys) {
        for (WALKey key : keys) {
            index.remove(key);
        }
    }

    @Override
    public void clear() {
        index.clear();
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {

        final MemoryWALIndex rowsIndex = new MemoryWALIndex();
        return new CompactionWALIndex() {

            @Override
            public void put(Collection<? extends Map.Entry<WALKey, WALValue>> entries) {
                rowsIndex.put(entries);
            }

            @Override
            public void abort() throws Exception {
            }

            @Override
            public void commit() throws Exception {
                index.clear();
                index.putAll(rowsIndex.index);
            }
        };

    }

}
