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
package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.service.storage.RowStoreUpdates;
import com.jivesoftware.os.amza.service.storage.TableStore;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;

public class AmzaTable {

    private final OrderIdProvider orderIdProvider;
    private final TableName tableName;
    private final TableStore tableStore;

    public AmzaTable(OrderIdProvider orderIdProvider, TableName tableName, TableStore tableStore) {
        this.tableName = tableName;
        this.orderIdProvider = orderIdProvider;
        this.tableStore = tableStore;
    }

    public TableName getTableName() {
        return tableName;
    }

    public RowIndexKey set(RowIndexKey key, byte[] value) throws Exception {
        if (value == null) {
            throw new IllegalStateException("Value cannot be null.");
        }
        RowStoreUpdates tx = tableStore.startTransaction(orderIdProvider.nextId());
        tx.add(key, value);
        tx.commit();
        return key;
    }

    public void set(Iterable<Entry<RowIndexKey, byte[]>> entries) throws Exception {
        RowStoreUpdates tx = tableStore.startTransaction(orderIdProvider.nextId());
        for (Entry<RowIndexKey, byte[]> e : entries) {
            RowIndexKey k = e.getKey();
            byte[] v = e.getValue();
            if (v == null) {
                throw new IllegalStateException("Value cannot be null.");
            }
            tx.add(k, v);
        }
        tx.commit();
    }

    public byte[] get(RowIndexKey key) throws Exception {
        return tableStore.get(key);
    }

    public List<byte[]> get(List<RowIndexKey> keys) throws Exception {
        List<byte[]> values = new ArrayList<>();
        for (RowIndexKey key : keys) {
            values.add(get(key));
        }
        return values;
    }

    // TODO replace ValueStream with RowScan
    public void get(Iterable<RowIndexKey> keys, ValueStream<Entry<RowIndexKey, byte[]>> valuesStream) throws Exception {
        for (final RowIndexKey key : keys) {
            final byte[] value = tableStore.get(key);
            if (value != null) {
                Entry<RowIndexKey, byte[]> entry = new Entry<RowIndexKey, byte[]>() {

                    @Override
                    public RowIndexKey getKey() {
                        return key;
                    }

                    @Override
                    public byte[] getValue() {
                        return value;
                    }

                    @Override
                    public byte[] setValue(byte[] value) {
                        return value;
                    }
                };
                if (valuesStream.stream(entry) != entry) {
                    break;
                }
            }
        }
        valuesStream.stream(null); //EOS
    }

    // TODO add concept of a key start stop and filtering
    public <E extends Exception> void scan(RowScan<E> stream) throws E {
        tableStore.rowScan(stream);
    }

    public static interface ValueStream<VV> {

        /**
         *
         * @param value null means end of stream.
         * @return implementor can stop the stream by returning null.
         */
        VV stream(VV value);
    }

    public boolean remove(RowIndexKey key) throws Exception {
        RowStoreUpdates tx = tableStore.startTransaction(orderIdProvider.nextId());
        tx.remove(key);
        tx.commit();
        return true;
    }

    public void remove(Iterable<RowIndexKey> keys) throws Exception {
        RowStoreUpdates tx = tableStore.startTransaction(orderIdProvider.nextId());
        for (RowIndexKey key : keys) {
            tx.remove(key);
        }
        tx.commit();
    }

    // TODO replace ValueStream with RowScan
    public void listEntries(final ValueStream<Entry<RowIndexKey, RowIndexValue>> stream) throws Exception {
        tableStore.rowScan(new RowScan<RuntimeException>() {

            @Override
            public boolean row(long orderId, final RowIndexKey key, final RowIndexValue value) {
                Entry e = new Entry() {

                    @Override
                    public RowIndexKey getKey() {
                        return key;
                    }

                    @Override
                    public RowIndexValue getValue() {
                        return value;
                    }

                    @Override
                    public Object setValue(Object value) {
                        throw new UnsupportedOperationException("Not supported.");
                    }
                };
                return stream.stream(e) != e;
            }
        });
        stream.stream(null); //EOS
    }

    public void takeRowUpdatesSince(long transationId, RowScan rowUpdates) throws Exception {
        tableStore.takeRowUpdatesSince(transationId, rowUpdates);
    }

    //  Use for testing
    public boolean compare(final AmzaTable amzaTable) throws Exception {
        final MutableInt compared = new MutableInt(0);
        final MutableBoolean passed = new MutableBoolean(true);
        amzaTable.tableStore.rowScan(new RowScan<RuntimeException>() {

            @Override
            public boolean row(long orderId, RowIndexKey key, RowIndexValue value) {
                try {
                    compared.increment();

                    RowIndexValue timestampedValue = tableStore.getTimestampedValue(key);
                    String comparing = tableName.getRingName() + ":" + tableName.getTableName()
                            + " to " + amzaTable.tableName.getRingName() + ":" + amzaTable.tableName.getTableName();

                    if (timestampedValue == null) {
                        System.out.println("INCONSISTENCY: " + comparing + " key:null"
                                + " != " + value.getTimestamp()
                                + "' -- " + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        return false;
                    }
                    if (value.getTimestamp() != timestampedValue.getTimestamp()) {
                        System.out.println("INCONSISTENCY: " + comparing + " timstamp:'" + timestampedValue.getTimestamp()
                                + "' != '" + value.getTimestamp()
                                + "' -- " + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        return false;
                    }
                    if (value.getTombstoned() != timestampedValue.getTombstoned()) {
                        System.out.println("INCONSISTENCY: " + comparing + " tombstone:" + timestampedValue.getTombstoned()
                                + " != '" + value.getTombstoned()
                                + "' -- " + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        return false;
                    }
                    if (value.getValue() == null && timestampedValue.getValue() != null) {
                        System.out.println("INCONSISTENCY: " + comparing + " null values:" + timestampedValue.getTombstoned()
                                + " != '" + value.getTombstoned()
                                + "' -- " + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        return false;
                    }
                    if (value.getValue() != null && !Arrays.equals(value.getValue(), timestampedValue.getValue())) {
                        System.out.println("INCONSISTENCY: " + comparing + " value:'" + timestampedValue.getValue()
                                + "' != '" + value.getValue()
                                + "' aClass:" + timestampedValue.getValue().getClass()
                                + "' bClass:" + value.getValue().getClass()
                                + "' -- " + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        return false;
                    }
                    return true;
                } catch (Exception x) {
                    throw new RuntimeException("Failed while comparing", x);
                }
            }
        });

        System.out.println("table:" + amzaTable.tableName.getTableName() + " compared:" + compared + " keys");
        return passed.booleanValue();
    }
}
