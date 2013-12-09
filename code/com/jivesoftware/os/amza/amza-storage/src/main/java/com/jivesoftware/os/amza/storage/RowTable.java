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
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RowTable implements RowsStorage {

    private final TableName tableName;
    private final OrderIdProvider orderIdProvider;
    private final RowsIndexProvider tableIndexProvider;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final RowReader<byte[]> rowReader;
    private final RowWriter<byte[]> rowWriter;
    private final AtomicReference<RowsIndex> tableIndex = new AtomicReference<>(null);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public RowTable(TableName tableName,
            OrderIdProvider orderIdProvider,
            RowsIndexProvider tableIndexProvider,
            RowMarshaller<byte[]> rowMarshaller,
            RowReader<byte[]> rowReader,
            RowWriter<byte[]> rowWriter) {
        this.tableName = tableName;
        this.orderIdProvider = orderIdProvider;
        this.tableIndexProvider = tableIndexProvider;
        this.rowMarshaller = rowMarshaller;
        this.rowReader = rowReader;
        this.rowWriter = rowWriter;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    synchronized public void load() throws Exception {
        final RowsIndex rowsIndex = tableIndexProvider.createRowsIndex(tableName);
        rowReader.scan(false, new RowReader.Stream<byte[]>() {
            @Override
            public boolean row(final byte[] rowPointer, byte[] row) throws Exception {
                rowMarshaller.fromRow(row, new RowScan() {

                    @Override
                    public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
                        RowIndexValue current = rowsIndex.get(key);
                        if (current == null) {
                            rowsIndex.put(key, new RowIndexValue(rowPointer, value.getTimestamp(), value.getTombstoned()));
                        } else if (current.getTimestamp() < value.getTimestamp()) {
                            rowsIndex.put(key, new RowIndexValue(rowPointer, value.getTimestamp(), value.getTombstoned()));
                        }
                        return true;
                    }
                });
                return true;
            }
        });
        tableIndex.set(rowsIndex);
    }

    @Override
    public RowsChanged update(RowScanable updates) throws Exception {
        final NavigableMap<RowIndexKey, RowIndexValue> appliedRows = new TreeMap<>();
        final NavigableMap<RowIndexKey, RowIndexValue> removedRows = new TreeMap<>();
        final Multimap<RowIndexKey, RowIndexValue> clobberedRows = ArrayListMultimap.create();
        final RowsIndex rowsIndex = tableIndex.get();
        updates.rowScan(new RowScan<RuntimeException>() {
            @Override
            public boolean row(long transactionId, RowIndexKey updateRowKey, RowIndexValue updateRowValue) {
                RowIndexValue currentRowValue = rowsIndex.get(updateRowKey);
                if (currentRowValue == null) {
                    appliedRows.put(updateRowKey, updateRowValue);
                } else {
                    if (updateRowValue.getTombstoned() && updateRowValue.getTimestamp() < 0) { // Handle tombstone updates
                        if (currentRowValue.getTimestamp() <= Math.abs(updateRowValue.getTimestamp())) {
                            RowIndexValue removeable = hydrateRowIndexValue(rowsIndex.get(updateRowKey));
                            if (removeable != null) {
                                removedRows.put(updateRowKey, removeable);
                                clobberedRows.put(updateRowKey, removeable);
                            }
                        }
                    } else if (currentRowValue.getTimestamp() < updateRowValue.getTimestamp()) {
                        clobberedRows.put(updateRowKey, currentRowValue);
                        appliedRows.put(updateRowKey, updateRowValue);
                    }
                }
                return true;
            }
        });
        if (!appliedRows.isEmpty()) {

            NavigableMap<RowIndexKey, RowIndexValue> saved = new TreeMap<>();
            NavigableMap<RowIndexKey, byte[]> keyToRowPointer = new TreeMap<>();
            synchronized (rowWriter) {
                long transactionId = 0;
                if (orderIdProvider != null) {
                    transactionId = orderIdProvider.nextId();
                }
                List<RowIndexKey> rowKeys = new ArrayList<>();
                List<byte[]> rows = new ArrayList<>();
                for (Map.Entry<RowIndexKey, RowIndexValue> e : appliedRows.entrySet()) {
                    RowIndexKey key = e.getKey();
                    RowIndexValue value = e.getValue();
                    byte[] toRow = rowMarshaller.toRow(transactionId, key, value);
                    rowKeys.add(key);
                    rows.add(toRow);
                    saved.put(key, value);
                }
                if (!rows.isEmpty()) {
                    List<byte[]> rowPointers = rowWriter.write(rows, true);
                    for (int i = 0; i < rowPointers.size(); i++) {
                        keyToRowPointer.put(rowKeys.get(i), rowPointers.get(i));
                    }
                }
            }
            try {
                readWriteLock.writeLock().lock();
                for (Map.Entry<RowIndexKey, RowIndexValue> entry : saved.entrySet()) {
                    RowIndexKey rowIndexKey = entry.getKey();
                    RowIndexValue rowIndexValue = entry.getValue();
                    byte[] rowPointer = keyToRowPointer.get(rowIndexKey);
                    RowIndexValue rowIndexValuePointer = new RowIndexValue(rowPointer,
                            rowIndexValue.getTimestamp(),
                            rowIndexValue.getTombstoned());
                    RowIndexValue got = rowsIndex.get(rowIndexKey);
                    if (got == null) {
                        rowsIndex.put(rowIndexKey, rowIndexValuePointer);
                    } else if (got.getTimestamp() < rowIndexValue.getTimestamp()) {
                        rowsIndex.put(rowIndexKey, rowIndexValuePointer);
                    }
                }
                NavigableMap<RowIndexKey, RowIndexValue> remove = removedRows;
                for (Map.Entry<RowIndexKey, RowIndexValue> entry : remove.entrySet()) {
                    RowIndexKey rowIndexKey = entry.getKey();
                    RowIndexValue timestampedValue = entry.getValue();
                    RowIndexValue got = rowsIndex.get(rowIndexKey);
                    if (got != null && got.getTimestamp() < timestampedValue.getTimestamp()) {
                        rowsIndex.remove(rowIndexKey);
                    }
                }

            } finally {
                readWriteLock.writeLock().unlock();
            }
            rowsIndex.commit();

            return new RowsChanged(tableName, saved, removedRows, clobberedRows);
        } else {
            return new RowsChanged(tableName, appliedRows, removedRows, clobberedRows);
        }
    }

    @Override
    public <E extends Exception> void rowScan(final RowScan<E> rowStream) throws E {
        RowsIndex rowsIndex = tableIndex.get();
        rowsIndex.rowScan(new RowScan<E>() {

            @Override
            public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws E {
                return rowStream.row(transactionId, key, hydrateRowIndexValue(value));
            }
        });
    }

    @Override
    public RowIndexValue get(RowIndexKey key) {
        RowsIndex rowsIndex = tableIndex.get();
        try {
            readWriteLock.readLock().lock();
            RowIndexValue got = rowsIndex.get(key);
            if (got == null) {
                return got;
            }
            return hydrateRowIndexValue(got);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(RowIndexKey key) {
        try {
            readWriteLock.readLock().lock();
            return tableIndex.get().containsKey(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private RowIndexValue hydrateRowIndexValue(RowIndexValue rowIndexValue) {
        try {
            // TODO replace with a read only pool of rowReaders
            byte[] row = rowReader.read(rowIndexValue.getValue());
            byte[] value = rowMarshaller.valueFromRow(row);
            return new RowIndexValue(value,
                    rowIndexValue.getTimestamp(),
                    rowIndexValue.getTombstoned());
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrtate " + rowIndexValue, x);
        }
    }

    @Override
    public void takeRowUpdatesSince(final long transactionId, final RowScan rowStream) throws Exception {
        final RowScan<Exception> filteringRowStream = new RowScan<Exception>() {
            @Override
            public boolean row(long orderId, RowIndexKey key, RowIndexValue value) throws Exception {
                if (orderId > transactionId) {
                    rowStream.row(orderId, key, value);
                }
                return true;
            }
        };
        rowReader.scan(true, new RowReader.Stream<byte[]>() {
            @Override
            public boolean row(byte[] rowPointer, byte[] row) throws Exception {
                return rowMarshaller.fromRow(row, filteringRowStream);
            }
        });
    }

    @Override
    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
//        System.out.println("TODO: compactTombestone ifOlderThanNMillis:" + ifOlderThanNMillis);
//        long[] unpack = new AmzaChangeIdPacker().unpack(ifOlderThanNMillis);
    }

    @Override
    public void clear() throws Exception {
        try {
            readWriteLock.writeLock().lock();
            List<byte[]> rows = new ArrayList<>();
            rowWriter.write(rows, false);

            RowsIndex rowsIndex = tableIndex.get();
            rowsIndex.clear();
            rowsIndex.commit();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
