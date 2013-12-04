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

import com.jivesoftware.os.amza.shared.MemoryTableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;

public class TableTransaction {

    private final TableStore sortedMapStore;
    private final ReadThroughChangeSet changesMap;
    private int changedCount = 0;

    TableTransaction(TableStore sortedMapStore,
            ReadThroughChangeSet updateableMap) {
        this.sortedMapStore = sortedMapStore;
        this.changesMap = updateableMap;
    }

    public TableIndexKey add(TableIndexKey key, byte[] value) throws Exception {
        if (changesMap.put(key, value)) {
            changedCount++;
        }
        return key;
    }

    public boolean remove(TableIndexKey key) throws Exception {
        if (changesMap.containsKey(key)) {
            byte[] got = changesMap.getValue(key);
            if (got != null) {
                if (changesMap.remove(key)) {
                    changedCount++;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public void commit() throws Exception {
        if (changedCount > 0) {
            sortedMapStore.commit(new MemoryTableIndex(changesMap.getChangesMap()));
        }
    }
}
