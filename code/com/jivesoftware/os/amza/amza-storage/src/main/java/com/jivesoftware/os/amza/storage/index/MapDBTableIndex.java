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

import com.jivesoftware.os.amza.shared.BinaryTimestampedValue;
import com.jivesoftware.os.amza.shared.EntryStream;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import java.util.Map.Entry;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class MapDBTableIndex implements TableIndex {

    private final DB db;
    private final BTreeMap<TableIndexKey, BinaryTimestampedValue> treeMap;

    public MapDBTableIndex(String mapName) {
        db = DBMaker.newDirectMemoryDB()
                .closeOnJvmShutdown()
                .make();
        treeMap = db.getTreeMap(mapName);
    }

    @Override
    public void flush() {
        db.commit();
    }

    @Override
    public <E extends Throwable> void entrySet(EntryStream<E> entryStream) {
        for (Entry<TableIndexKey, BinaryTimestampedValue> e : treeMap.entrySet()) {
            try {
                if (!entryStream.stream(e.getKey(), e.getValue())) {
                    break;
                }
            } catch (Throwable t) {
                throw new RuntimeException("Failed while streaming entry set.", t);
            }
        }
    }

    @Override
    public boolean isEmpty() {
        return treeMap.isEmpty();
    }

    @Override
    public boolean containsKey(TableIndexKey key) {
        return treeMap.containsKey(key);
    }

    @Override
    public BinaryTimestampedValue get(TableIndexKey key) {
        return treeMap.get(key);
    }

    @Override
    public BinaryTimestampedValue put(TableIndexKey key,
            BinaryTimestampedValue value) {
        return treeMap.put(key, value);
    }

    @Override
    public BinaryTimestampedValue remove(TableIndexKey key) {
        return treeMap.remove(key);
    }

    @Override
    public void clear() {
        treeMap.clear();
    }
}
