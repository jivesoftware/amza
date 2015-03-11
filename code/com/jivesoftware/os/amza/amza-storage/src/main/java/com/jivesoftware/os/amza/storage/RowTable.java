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

import com.google.common.base.Optional;
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
import com.jivesoftware.os.amza.shared.RowsStorage;
import com.jivesoftware.os.amza.shared.RowsTx;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RowTable implements RowsStorage {

    private final TableName tableName;
    private final OrderIdProvider orderIdProvider;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final RowsTx<byte[]> rowsTx;
    private final AtomicReference<RowsIndex> tableIndex = new AtomicReference<>(null);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(); // TODO could be replaced with a semaphore and avoid RWL's threadlocal

    public RowTable(TableName tableName,
        OrderIdProvider orderIdProvider,
        RowMarshaller<byte[]> rowMarshaller,
        RowsTx<byte[]> rowsTx) {
        this.tableName = tableName;
        this.orderIdProvider = orderIdProvider;
        this.rowMarshaller = rowMarshaller;
        this.rowsTx = rowsTx;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public void compactTombstone(long removeTombstonedOlderThanTimestampId) throws Exception {

        Optional<RowsTx.Compacted> compact = rowsTx.compact(tableName, removeTombstonedOlderThanTimestampId, tableIndex.get());

        if (compact.isPresent()) {
            readWriteLock.writeLock().lock();
            try {
                RowsIndex compactedRowsIndex = compact.get().getCompactedRowsIndex();
                tableIndex.set(compactedRowsIndex);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    @Override
    public void load() throws Exception {
        readWriteLock.readLock().lock();
        try {
            tableIndex.compareAndSet(null, rowsTx.load(tableName));
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public RowsChanged update(RowScanable updates) throws Exception {
        readWriteLock.writeLock().lock();
        try {
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
                        if (updateRowValue.getTombstoned() && updateRowValue.getTimestampId() < 0) { // Handle tombstone updates
                            if (currentRowValue.getTimestampId() <= Math.abs(updateRowValue.getTimestampId())) {
                                RowIndexValue removeable = hydrateRowIndexValue(rowsIndex.get(updateRowKey));
                                if (removeable != null) {
                                    removedRows.put(updateRowKey, removeable);
                                    clobberedRows.put(updateRowKey, removeable);
                                }
                            }
                        } else if (currentRowValue.getTimestampId() < updateRowValue.getTimestampId()) {
                            clobberedRows.put(updateRowKey, currentRowValue);
                            appliedRows.put(updateRowKey, updateRowValue);
                        }
                    }
                    return true;
                }
            });
            if (!appliedRows.isEmpty()) {

                final NavigableMap<RowIndexKey, RowIndexValue> saved = new TreeMap<>();
                final NavigableMap<RowIndexKey, byte[]> keyToRowPointer = new TreeMap<>();

                rowsTx.write(new RowsTx.RowsWrite<byte[], Void>() {

                    @Override
                    public Void write(RowWriter<byte[]> rowWriter) throws Exception {
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
                        return null;
                    }
                });

                for (Map.Entry<RowIndexKey, RowIndexValue> entry : saved.entrySet()) {
                    RowIndexKey rowIndexKey = entry.getKey();
                    RowIndexValue rowIndexValue = entry.getValue();
                    byte[] rowPointer = keyToRowPointer.get(rowIndexKey);
                    RowIndexValue rowIndexValuePointer = new RowIndexValue(rowPointer,
                        rowIndexValue.getTimestampId(),
                        rowIndexValue.getTombstoned());
                    RowIndexValue got = rowsIndex.get(rowIndexKey);
                    if (got == null) {
                        rowsIndex.put(rowIndexKey, rowIndexValuePointer);
                    } else if (got.getTimestampId() < rowIndexValue.getTimestampId()) {
                        rowsIndex.put(rowIndexKey, rowIndexValuePointer);
                    }
                }
                NavigableMap<RowIndexKey, RowIndexValue> remove = removedRows;
                for (Map.Entry<RowIndexKey, RowIndexValue> entry : remove.entrySet()) {
                    RowIndexKey rowIndexKey = entry.getKey();
                    RowIndexValue timestampedValue = entry.getValue();
                    RowIndexValue got = rowsIndex.get(rowIndexKey);
                    if (got != null && got.getTimestampId() < timestampedValue.getTimestampId()) {
                        rowsIndex.remove(rowIndexKey);
                    }
                }

                return new RowsChanged(tableName, saved, removedRows, clobberedRows);
            } else {
                return new RowsChanged(tableName, appliedRows, removedRows, clobberedRows);
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public <E extends Exception> void rowScan(final RowScan<E> rowStream) throws E {
        readWriteLock.readLock().lock();
        try {
            RowsIndex rowsIndex = tableIndex.get();
            rowsIndex.rowScan(new RowScan<E>() {

                @Override
                public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws E {
                    return rowStream.row(transactionId, key, hydrateRowIndexValue(value));
                }
            });
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public <E extends Exception> void rangeScan(final RowIndexKey from, final RowIndexKey to, final RowScan<E> rowScan) throws E {
        readWriteLock.readLock().lock();
        try {
            RowsIndex rowsIndex = tableIndex.get();
            rowsIndex.rangeScan(from, to, new RowScan<E>() {

                @Override
                public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws E {
                    return rowScan.row(transactionId, key, hydrateRowIndexValue(value));
                }
            });
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public RowIndexValue get(RowIndexKey key) {
        readWriteLock.readLock().lock();
        try {
            RowsIndex rowsIndex = tableIndex.get();
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
    public List<RowIndexValue> get(List<RowIndexKey> keys) {
        List<RowIndexValue> gots = new ArrayList<>(keys.size());
        readWriteLock.readLock().lock();
        try {
            RowsIndex rowsIndex = tableIndex.get();
            for (RowIndexKey key : keys) {
                RowIndexValue got = rowsIndex.get(key);
                if (got == null) {
                    gots.add(null);
                } else {
                    gots.add(hydrateRowIndexValue(got));
                }
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
        return gots;
    }

    @Override
    public boolean containsKey(RowIndexKey key) {
        readWriteLock.readLock().lock();
        try {
            return tableIndex.get().containsKey(key);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public List<Boolean> containsKey(List<RowIndexKey> keys) {
        List<Boolean> contains = new ArrayList<>(keys.size());
        readWriteLock.readLock().lock();
        try {
            RowsIndex rowsIndex = tableIndex.get();
            for (RowIndexKey key : keys) {
                contains.add(rowsIndex.containsKey(key));
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
        return contains;
    }

    private RowIndexValue hydrateRowIndexValue(final RowIndexValue rowIndexValue) {
        try {
            // TODO replace with a read only pool of rowReaders
            byte[] row = rowsTx.read(new RowsTx.RowsRead<byte[], byte[]>() {

                @Override
                public byte[] read(RowReader<byte[]> rowReader) throws Exception {
                    return rowReader.read(rowIndexValue.getValue());
                }
            });
            byte[] value = rowMarshaller.valueFromRow(row);
            return new RowIndexValue(value,
                rowIndexValue.getTimestampId(),
                rowIndexValue.getTombstoned());
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrtate " + rowIndexValue, x);
        }
    }

    @Override
    public void takeRowUpdatesSince(final long sinceTransactionId, final RowScan rowStream) throws Exception {
        readWriteLock.readLock().lock();
        try {
            final AtomicLong took = new AtomicLong();
            final RowScan<Exception> filteringRowStream = new RowScan<Exception>() {
                @Override
                public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
                    if (transactionId > sinceTransactionId) {
                        took.incrementAndGet();
                        if (!rowStream.row(transactionId, key, value)) {
                            return false;
                        }
                    }
                    return transactionId > sinceTransactionId;
                }
            };

            rowsTx.read(new RowsTx.RowsRead<byte[], Void>() {

                @Override
                public Void read(RowReader<byte[]> rowReader) throws Exception {
                    rowReader.reverseScan(new RowReader.Stream<byte[]>() {
                        @Override
                        public boolean row(long rowPointer, byte[] row) throws Exception {
                            return rowMarshaller.fromRow(row, filteringRowStream);
                        }
                    });
                    return null;
                }
            });

            if (took.longValue() > 0) {
                System.out.println("Took:" + took.longValue() + " " + sinceTransactionId + " " + tableName);
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void clear() throws Exception {
        readWriteLock.writeLock().lock();
        try {
            RowsIndex rowsIndex = tableIndex.get();
            rowsIndex.clear();

            final List<byte[]> rows = new ArrayList<>();
            rowsTx.write(new RowsTx.RowsWrite<byte[], Void>() {

                @Override
                public Void write(RowWriter<byte[]> rowWriter) throws Exception {
                    synchronized (rowWriter) {
                        rowWriter.write(rows, false);
                    }
                    return null;
                }
            });

        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

}
