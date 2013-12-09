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

    private final NavigableMap<RowIndexKey, RowIndexValue> index;
    private final Flusher flusher;

    public MemoryRowsIndex(NavigableMap<RowIndexKey, RowIndexValue> index, Flusher flusher) throws Exception {
        this.index = index;
        this.flusher = flusher;
    }

    @Override
    public void commit() {
        if (flusher != null) {
            flusher.flush();
        }
    }

    @Override
    public void compact() {
        if (flusher != null) {
            //flusher.compact(); // TODO?
        }
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
        return got;
    }

    @Override
    public RowIndexValue put(RowIndexKey key, RowIndexValue value) {
        RowIndexValue had = index.put(key, value);
        if (had == null) {
            return null;
        }
        return had;
    }

    @Override
    public RowIndexValue remove(RowIndexKey key) {
        RowIndexValue removed = index.remove(key);
        if (removed == null) {
            return null;
        }
        return removed;
    }

    @Override
    public void clear() {
        index.clear();
    }


}
