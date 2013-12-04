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
package com.jivesoftware.os.amza.storage;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.amza.shared.BinaryTimestampedValue;
import com.jivesoftware.os.amza.shared.EntryStream;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class FileBackedTableStorage<R> implements TableStorage {

    private final RowTable<R> rowTable;

    public FileBackedTableStorage(RowTable<R> rowTable) {
        this.rowTable = rowTable;
    }

    @Override
    public TableName getTableName() {
        return rowTable.getTableName();
    }

    @Override
    synchronized public TableIndex load() throws Exception {
        return rowTable.load();
    }

    @Override
    public void rowMutationSince(final long transactionId, TransactionSetStream transactionSetStream) throws Exception {
        rowTable.rowMutationSince(transactionId, transactionSetStream);
    }

    @Override
    synchronized public void clear() throws Exception {
        ConcurrentNavigableMap<TableIndexKey, BinaryTimestampedValue> saveableMap = new ConcurrentSkipListMap<>();
//        TableIndex<K, V> load = load();
//        Multimap<K, TimestampedValue> all = ArrayListMultimap.create();
//        for (Entry<K, TimestampedValue> entry : load.entrySet()) {
//            all.put(entry.getKey(), entry.getValue());
//        }
        rowTable.save(null, saveableMap, false); // TODO should we add a clear to  RowTable interface
    }

    @Override
    synchronized public TableDelta update(TableIndex mutatedRows,
            final TableIndex allRows) throws Exception {

        final NavigableMap<TableIndexKey, BinaryTimestampedValue> applyMap = new TreeMap<>();
        final NavigableMap<TableIndexKey, BinaryTimestampedValue> removeMap = new TreeMap<>();
        final Multimap<TableIndexKey, BinaryTimestampedValue> clobberedRows = ArrayListMultimap.create();
        mutatedRows.entrySet(new EntryStream<RuntimeException>() {
            @Override
            public boolean stream(TableIndexKey key, BinaryTimestampedValue update) {
                BinaryTimestampedValue current = allRows.get(key);
                if (current == null) {
                    applyMap.put(key, update);
                } else {
                    if (update.getTombstoned() && update.getTimestamp() < 0) { // Handle tombstone updates
                        if (current.getTimestamp() <= Math.abs(update.getTimestamp())) {
                            BinaryTimestampedValue removeable = allRows.get(key);
                            if (removeable != null) {
                                removeMap.put(key, removeable);
                                clobberedRows.put(key, removeable);
                            }
                        }
                    } else if (current.getTimestamp() < update.getTimestamp()) {
                        clobberedRows.put(key, current);
                        applyMap.put(key, update);
                    }
                }
                return true;
            }
        });
        if (!applyMap.isEmpty()) {
            NavigableMap<TableIndexKey, BinaryTimestampedValue> saved = rowTable.save(clobberedRows, applyMap, true);
            return new TableDelta(saved, removeMap, clobberedRows);
        } else {
            return new TableDelta(applyMap, removeMap, clobberedRows);
        }
    }

    @Override
    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        rowTable.compactTombestone(ifOlderThanNMillis);
    }
}
