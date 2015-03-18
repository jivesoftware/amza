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

public class MemoryRowsIndex implements RowsIndex, Serializable {

    private final NavigableMap<RowIndexKey, RowIndexValue> index;

    public MemoryRowsIndex() {
        this(new ConcurrentSkipListMap<RowIndexKey, RowIndexValue>());
    }

    public MemoryRowsIndex(NavigableMap<RowIndexKey, RowIndexValue> index) {
        this.index = index;
    }

    @Override
    public void commit() {

    }

    @Override
    public void compact() {

    }

    @Override
    public <E extends Exception> void rowScan(RowScan<E> rowScan) throws Exception {
        for (Entry<RowIndexKey, RowIndexValue> e : index.entrySet()) {
            RowIndexKey key = e.getKey();
            RowIndexValue value = e.getValue();
            if (!rowScan.row(-1, key, value)) {
                break;
            }
        }
    }

    @Override
    public <E extends Exception> void rangeScan(RowIndexKey from, RowIndexKey to, RowScan<E> rowScan) throws E {
        for (Entry<RowIndexKey, RowIndexValue> e : index.subMap(from, to).entrySet()) {
            RowIndexKey key = e.getKey();
            RowIndexValue value = e.getValue();
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
    public List<Boolean> containsKey(List<RowIndexKey> keys) {
        List<Boolean> contains = new ArrayList<>(keys.size());
        for (RowIndexKey key : keys) {
            contains.add(index.containsKey(key));
        }
        return contains;
    }

    @Override
    public List<RowIndexValue> get(List<RowIndexKey> keys) {
        List<RowIndexValue> gots = new ArrayList<>(keys.size());
        for (RowIndexKey key : keys) {
            gots.add(index.get(key));
        }
        return gots;
    }

    @Override
    public void put(Collection<? extends Map.Entry<RowIndexKey, RowIndexValue>> entrys) {
        for (Map.Entry<RowIndexKey, RowIndexValue> entry : entrys) {
            index.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(Collection<RowIndexKey> keys) {
        for (RowIndexKey key : keys) {
            index.remove(key);
        }
    }

    @Override
    public void clear() {
        index.clear();
    }

    @Override
    public CompactionRowIndex startCompaction() throws Exception {

        final MemoryRowsIndex rowsIndex = new MemoryRowsIndex();
        return new CompactionRowIndex() {

            @Override
            public void put(Collection<? extends Map.Entry<RowIndexKey, RowIndexValue>> entries) {
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
