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
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableIndexProvider;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableRowReader;
import com.jivesoftware.os.amza.shared.TableRowWriter;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.commons.lang.mutable.MutableLong;

public class RowTable<R> {

    private final TableName tableName;
    private final OrderIdProvider orderIdProvider;
    private final TableIndexProvider tableIndexProvider;
    private final RowMarshaller<R> rowMarshaller;
    private final TableRowReader<R> rowReader;
    private final TableRowWriter<R> rowWriter;

    public RowTable(TableName tableName,
            OrderIdProvider orderIdProvider,
            TableIndexProvider tableIndexProvider,
            RowMarshaller<R> rowMarshaller,
            TableRowReader<R> rowReader,
            TableRowWriter<R> rowWriter) {
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

    synchronized public TableIndex load() throws Exception {
        final TableIndex tableIndex = tableIndexProvider.createTableIndex(tableName);
        final Multimap<TableIndexKey, BinaryTimestampedValue> was = ArrayListMultimap.create();
        rowReader.read(false, new TableRowReader.Stream<R>() {
            @Override
            public boolean stream(R row) throws Exception {
                TransactionEntry transactionEntry = rowMarshaller.fromRow(row);
                TableIndexKey key = transactionEntry.getKey();
                BinaryTimestampedValue timestampedValue = transactionEntry.getValue();
                BinaryTimestampedValue current = tableIndex.get(key);
                if (current == null) {
                    tableIndex.put(key, timestampedValue);
                } else if (current.getTimestamp() < timestampedValue.getTimestamp()) {
                    was.put(key, current);
                    tableIndex.put(key, timestampedValue);
                }
                return true;
            }
        });
        //save(was, tableIndex, false); // write compacted table after load.
        return tableIndex;
    }

    public NavigableMap<TableIndexKey, BinaryTimestampedValue> save(Multimap<TableIndexKey, BinaryTimestampedValue> was,
            NavigableMap<TableIndexKey, BinaryTimestampedValue> mutation, boolean append) throws Exception {
        synchronized (rowWriter) {
            long transactionId = 0;
            if (orderIdProvider != null) {
                transactionId = orderIdProvider.nextId();
            }
            NavigableMap<TableIndexKey, BinaryTimestampedValue> saved = new TreeMap<>();
            List<R> rows = new ArrayList<>();
            for (Map.Entry<TableIndexKey, BinaryTimestampedValue> e : mutation.entrySet()) {
                R toRow = rowMarshaller.toRow(transactionId, e.getKey(), e.getValue());
                rows.add(toRow);
                saved.put(e.getKey(), rowMarshaller.fromRow(toRow).getValue());
            }
            rowWriter.write(rows, append);
            return saved;
        }
    }

    synchronized public void rowMutationSince(final long transactionId, TransactionSetStream transactionSetStream) throws Exception {
        final MutableLong higestOrderId = new MutableLong(transactionId);
        final ConcurrentSkipListMap<TableIndexKey, BinaryTimestampedValue> changes = new ConcurrentSkipListMap<>();
        rowReader.read(true, new TableRowReader.Stream<R>() {
            @Override
            public boolean stream(R row) throws Exception {
                TransactionEntry transactionEntry = rowMarshaller.fromRow(row);
                if (transactionEntry.getOrderId() <= transactionId) {
                    return false;
                }
                BinaryTimestampedValue had = changes.putIfAbsent(transactionEntry.getKey(), transactionEntry.getValue());
                if (had != null && had.getTimestamp() > transactionEntry.getValue().getTimestamp()) {
                    changes.put(transactionEntry.getKey(), had);
                }
                higestOrderId.setValue(Math.max(higestOrderId.longValue(), transactionEntry.getOrderId()));
                return true;
            }
        });
        transactionSetStream.stream(new TransactionSet(higestOrderId.longValue(), changes));
    }

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
}
