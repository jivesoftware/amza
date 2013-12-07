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
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowReader;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowScanable;
import com.jivesoftware.os.amza.shared.RowWriter;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.RowsIndex;
import com.jivesoftware.os.amza.shared.RowsIndexProvider;
import com.jivesoftware.os.amza.shared.RowsStorage;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.ValueStorage;
import com.jivesoftware.os.amza.shared.ValueStorageProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

public class RowTable<R> implements RowsStorage {

    private final TableName tableName;
    private final OrderIdProvider orderIdProvider;
    private final RowsIndexProvider tableIndexProvider;
    private final ValueStorageProvider valueStorageProvider;
    private final RowMarshaller<R> rowMarshaller;
    private final RowReader<R> rowReader;
    private final RowWriter<R> rowWriter;
    private final AtomicReference<ValueStorage> valueStorage = new AtomicReference<>(null);
    private final AtomicReference<RowsIndex> tableIndex = new AtomicReference<>(null);

    public RowTable(TableName tableName,
            OrderIdProvider orderIdProvider,
            RowsIndexProvider tableIndexProvider,
            ValueStorageProvider valueStorageProvider,
            RowMarshaller<R> rowMarshaller,
            RowReader<R> rowReader,
            RowWriter<R> rowWriter) {
        this.tableName = tableName;
        this.orderIdProvider = orderIdProvider;
        this.tableIndexProvider = tableIndexProvider;
        this.valueStorageProvider = valueStorageProvider;
        this.rowMarshaller = rowMarshaller;
        this.rowReader = rowReader;
        this.rowWriter = rowWriter;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    synchronized public void load() throws Exception {
        final ValueStorage storage = valueStorageProvider.createValueStorage(tableName);
        final RowsIndex index = tableIndexProvider.createRowsIndex(tableName, storage);
        final Multimap<RowIndexKey, RowIndexValue> was = ArrayListMultimap.create();
        final RowScan<RuntimeException> rowStream = new RowScan<RuntimeException>() {
            @Override
            public boolean row(long orderId, RowIndexKey key, RowIndexValue value) throws RuntimeException {
                RowIndexValue current = index.get(key);
                if (current == null) {
                    index.put(key, value);
                } else if (current.getTimestamp() < value.getTimestamp()) {
                    was.put(key, current);
                    index.put(key, value);
                }
                return true;
            }
        };
        rowReader.read(false, new RowReader.Stream<R>() {
            @Override
            public boolean stream(R row) throws Exception {
                rowMarshaller.fromRow(row, rowStream);
                return true;
            }
        });
        //save(was, tableIndex, false); // write compacted table after load.
        valueStorage.set(storage);
        tableIndex.set(index);
    }

    @Override
    public RowsChanged update(RowScanable update) throws Exception {
        final NavigableMap<RowIndexKey, RowIndexValue> applyMap = new TreeMap<>();
        final NavigableMap<RowIndexKey, RowIndexValue> removeMap = new TreeMap<>();
        final Multimap<RowIndexKey, RowIndexValue> clobberedRows = ArrayListMultimap.create();
        final RowsIndex rowIndex = tableIndex.get();
        update.rowScan(new RowScan<RuntimeException>() {
            @Override
            public boolean row(long orderId, RowIndexKey key, RowIndexValue update) {
                RowIndexValue current = rowIndex.get(key);
                if (current == null) {
                    applyMap.put(key, update);
                } else {
                    if (update.getTombstoned() && update.getTimestamp() < 0) { // Handle tombstone updates
                        if (current.getTimestamp() <= Math.abs(update.getTimestamp())) {
                            RowIndexValue removeable = rowIndex.get(key);
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
            //System.out.println("applyMap:" + applyMap.size() + " removeMap:" + removeMap.size() + " clobberedRows:" + clobberedRows.size());
            NavigableMap<RowIndexKey, RowIndexValue> saved = save(clobberedRows, applyMap, true);
            return new RowsChanged(tableName, saved, removeMap, clobberedRows);
        } else {
            return new RowsChanged(tableName, applyMap, removeMap, clobberedRows);
        }
    }

    @Override
    public <E extends Exception> void rowScan(RowScan<E> rowStream) throws E {
        tableIndex.get().rowScan(rowStream);
    }

    @Override
    public void put(RowIndexKey k, RowIndexValue timestampedValue) {
        tableIndex.get().put(k, timestampedValue);
    }

    @Override
    public RowIndexValue get(RowIndexKey key) {
        return tableIndex.get().get(key);
    }

    @Override
    public boolean containsKey(RowIndexKey key) {
        return tableIndex.get().containsKey(key);
    }


    @Override
    public void remove(RowIndexKey key) {
        tableIndex.get().remove(key);
    }

    public NavigableMap<RowIndexKey, RowIndexValue> save(Multimap<RowIndexKey, RowIndexValue> was,
            NavigableMap<RowIndexKey, RowIndexValue> mutation,
            boolean append) throws Exception {
        synchronized (rowWriter) {
            long transactionId = 0;
            if (orderIdProvider != null) {
                transactionId = orderIdProvider.nextId();
            }
            NavigableMap<RowIndexKey, RowIndexValue> saved = new TreeMap<>();
            List<R> rows = new ArrayList<>();
            for (Map.Entry<RowIndexKey, RowIndexValue> e : mutation.entrySet()) {
                RowIndexKey key = e.getKey();
                RowIndexValue value = e.getValue();
                R toRow = rowMarshaller.toRow(transactionId, key, value);
                rows.add(toRow);
                saved.put(key, value); // TODO what value do we want the pointer or the real value?
            }
            if (!rows.isEmpty()) {
                rowWriter.write(rows, append);
            }
            return saved;
        }
    }

    @Override
    synchronized public void takeRowUpdatesSince(final long transactionId, final RowScan rowStream) throws Exception {
        final RowScan<Exception> filteringRowStream = new RowScan<Exception>() {
            @Override
            public boolean row(long orderId, RowIndexKey key, RowIndexValue value) throws Exception {
                if (orderId > transactionId) {
                    byte[] storeValue = valueStorage.get().get(value.getValue());
                    rowStream.row(orderId, key, new RowIndexValue(storeValue, value.getTimestamp(), value.getTombstoned()));
                }
                return true;
            }
        };

        rowReader.read(true, new RowReader.Stream<R>() {
            @Override
            public boolean stream(R row) throws Exception {
                return rowMarshaller.fromRow(row, filteringRowStream);
            }
        });
    }

    @Override
    synchronized public void compactTombestone(long ifOlderThanNMillis) throws Exception {
//        System.out.println("TODO: compactTombestone ifOlderThanNMillis:" + ifOlderThanNMillis);
//        long[] unpack = new AmzaChangeIdPacker().unpack(ifOlderThanNMillis);
//        ConcurrentNavigableMap<K, TimestampedValue> changes = new ConcurrentSkipListMap<>();
//        ConcurrentNavigableMap<K, TimestampedValue> currentMap = readMap.get();
//        for (Map.Entry<K, TimestampedValue> e : currentMap.entrySet()) {
//            TimestampedValue timestampedValue = e.getValue();
//            if (timestampedValue.getTimestamp() < ifOlderThanTimestamp) {
//                changes.put(e.getKey(), new TimestampedValue(null, -timestampedValue.getTimestamp(), true));
//            }
//        }
//        if (!changes.isEmpty()) {
//            applyChanges(changes);
//        }
    }

    @Override
    synchronized public void clear() throws Exception {
        ConcurrentNavigableMap<RowIndexKey, RowIndexValue> saveableMap = new ConcurrentSkipListMap<>();
//        TableIndex<K, V> load = load();
//        Multimap<K, TimestampedValue> all = ArrayListMultimap.create();
//        for (Entry<K, TimestampedValue> entry : load.entrySet()) {
//            all.put(entry.getKey(), entry.getValue());
//        }
        save(null, saveableMap, false);
    }



}
