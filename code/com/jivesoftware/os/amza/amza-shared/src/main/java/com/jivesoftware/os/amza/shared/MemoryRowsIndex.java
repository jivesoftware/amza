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

public class MemoryRowsIndex implements RowsIndex, Serializable {

    private final NavigableMap<RowIndexKey, RowIndexValue> rowsIndex;

    public MemoryRowsIndex(NavigableMap<RowIndexKey, RowIndexValue> rowsIndex) {
        this.rowsIndex = rowsIndex;
    }

    @Override
    public RowIndexValue put(RowIndexKey key, RowIndexValue value) {
        return rowsIndex.put(key, value);
    }

    @Override
    public RowIndexValue get(RowIndexKey key) {
        return rowsIndex.get(key);
    }

    @Override
    public boolean containsKey(RowIndexKey key) {
        return rowsIndex.containsKey(key);
    }

    @Override
    public RowIndexValue remove(RowIndexKey key) {
        return rowsIndex.remove(key);
    }

    @Override
    public <E extends Exception> void rowScan(RowScan<E> rowScan) {
        for (Entry<RowIndexKey, RowIndexValue> e : rowsIndex.entrySet()) {
            try {
                if (!rowScan.row(-1, e.getKey(), e.getValue())) {
                    break;
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Error while streaming entry set.", ex);
            }
        }
    }

    @Override
    public void clear() {
        rowsIndex.clear();
    }

    @Override
    public boolean isEmpty() {
        return rowsIndex.isEmpty();
    }

    @Override
    public void commit() {
    }

    @Override
    public void compact() {
    }
}
