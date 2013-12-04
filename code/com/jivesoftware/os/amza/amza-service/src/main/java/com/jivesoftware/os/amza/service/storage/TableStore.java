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

import com.jivesoftware.os.amza.shared.BasicTimestampedValue;
import com.jivesoftware.os.amza.shared.KeyValueFilter;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import java.util.concurrent.ConcurrentNavigableMap;

public class TableStore {

    private final ReadWriteTableStore readWriteMaps;

    public TableStore(ReadWriteTableStore readWriteTable) {
        this.readWriteMaps = readWriteTable;
    }

    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        readWriteMaps.compactTombestone(ifOlderThanNMillis);
    }

    public ConcurrentNavigableMap<TableIndexKey, TimestampedValue> filter(final KeyValueFilter filter) throws Exception {
        final ConcurrentNavigableMap<TableIndexKey, TimestampedValue> results = filter.createCollector();
        readWriteMaps.getImmutableCopy().entrySet(new TableIndex.EntryStream<RuntimeException>() {

            @Override
            public boolean stream(TableIndexKey key, TimestampedValue value) {
                if (!value.getTombstoned()) {
                    byte[] v = value.getValue();
                    if (filter.filter(key, v)) {
                        results.put(key, new BasicTimestampedValue(v, value.getTimestamp(), value.getTombstoned()));
                    }
                }
                return true;
            }
        });
        return results;
    }

    public byte[] getValue(TableIndexKey k) throws Exception {
        return readWriteMaps.get(k);
    }

    public TimestampedValue getTimestampedValue(TableIndexKey k) throws Exception {
        return readWriteMaps.getTimestampedValue(k);
    }

    public TableIndex getImmutableRows() throws Exception {
        return readWriteMaps.getImmutableCopy();
    }

    public void getMutatedRowsSince(long transactionId, TransactionSetStream transactionSetStream) throws Exception {
        readWriteMaps.getMutatedRowsSince(transactionId, transactionSetStream);
    }

    public void clearAllRows() throws Exception {
        readWriteMaps.clear();
    }

    public void commit(TableIndex changes) throws Exception {
        readWriteMaps.commit(changes);
    }

    public TableTransaction startTransaction(long timestamp) throws Exception {
        return new TableTransaction(this, readWriteMaps.getReadThroughChangeSet(timestamp));
    }
}