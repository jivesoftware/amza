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

import com.jivesoftware.os.amza.shared.KeyValueFilter;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class TableStore<K, V> {

    private final ReadWriteTableStore<K, V> readWriteMaps;

    public TableStore(ReadWriteTableStore<K, V> readWriteTable) {
        this.readWriteMaps = readWriteTable;
    }

    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        readWriteMaps.compactTombestone(ifOlderThanNMillis);
    }

    public ConcurrentNavigableMap<K, TimestampedValue<V>> filter(KeyValueFilter<K, V> filter) throws Exception {
        ConcurrentNavigableMap<K, TimestampedValue<V>> results = filter.createCollector();
        for (Map.Entry<K, TimestampedValue<V>> e : readWriteMaps.getImmutableCopy().entrySet()) {
            if (e.getValue().getTombstoned()) {
                continue;
            }
            if (filter.filter(e.getKey(), e.getValue().getValue())) {
                results.put(e.getKey(), e.getValue());
            }
        }
        return results;
    }

    public V getValue(K k) throws Exception {
        return readWriteMaps.get(k);
    }

    public TimestampedValue<V> getTimestampedValue(K k) throws Exception {
        return readWriteMaps.getTimestampedValue(k);
    }

    public NavigableMap<K, TimestampedValue<V>> getImmutableRows() throws Exception {
        return readWriteMaps.getImmutableCopy();
    }

    public void getMutatedRowsSince(long transactionId, TransactionSetStream<K, V> transactionSetStream) throws Exception {
        readWriteMaps.getMutatedRowsSince(transactionId, transactionSetStream);
    }

    public void clearAllRows() throws Exception {
        readWriteMaps.clear();
    }

    public void commit(NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        readWriteMaps.commit(changes);
    }

    public TableTransaction<K, V> startTransaction(long timestamp) throws Exception {
        return new TableTransaction<>(this, readWriteMaps.getReadThroughChangeSet(timestamp));
    }
}