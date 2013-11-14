package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.service.storage.TableStore;
import com.jivesoftware.os.amza.service.storage.TableTransaction;
import com.jivesoftware.os.amza.shared.KeyValueFilter;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class AmzaTable<K, V> {

    private final OrderIdProvider orderIdProvider;
    private final TableName<K, V> tableName;
    private final TableStore<K, V> tableStore;

    public AmzaTable(OrderIdProvider orderIdProvider, TableName tableName, TableStore<K, V> tableStore) {
        this.tableName = tableName;
        this.orderIdProvider = orderIdProvider;
        this.tableStore = tableStore;
    }

    public TableName<K, V> getTableName() {
        return tableName;
    }

    public K set(K key, V value) throws Exception {
        if (value == null) {
            throw new IllegalStateException("Value cannot be null.");
        }
        TableTransaction<K, V> tx = tableStore.startTransaction(orderIdProvider.nextId());
        tx.add(key, value);
        tx.commit();
        return key;
    }

    public void set(Iterable<Entry<K, V>> entries) throws Exception {
        TableTransaction<K, V> tx = tableStore.startTransaction(orderIdProvider.nextId());
        for (Entry<K, V> e : entries) {
            K k = e.getKey();
            V v = e.getValue();
            if (v == null) {
                throw new IllegalStateException("Value cannot be null.");
            }
            tx.add(k, v);
        }
        tx.commit();
    }

    public V get(K key) throws Exception {
        return tableStore.getValue(key);
    }

    public List<V> get(List<K> keys) throws Exception {
        List<V> values = new ArrayList<>();
        for (K key : keys) {
            values.add(get(key));
        }
        return values;
    }

    public void get(Iterable<K> keys, ValueStream<Entry<K, V>> valuesStream) throws Exception {
        for (final K key : keys) {
            final V value = tableStore.getValue(key);
            if (value != null) {
                Entry<K, V> entry = new Entry<K, V>() {

                    @Override
                    public K getKey() {
                        return key;
                    }

                    @Override
                    public V getValue() {
                        return value;
                    }

                    @Override
                    public V setValue(V value) {
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

    public ConcurrentNavigableMap<K, TimestampedValue<V>> filter(KeyValueFilter<K, V> filter) throws Exception {
        return tableStore.filter(filter);
    }

    public static interface ValueStream<VV> {

        /**
         *
         * @param value null means end of stream.
         * @return implementor can stop the stream by returning null.
         */
        VV stream(VV value);
    }

    public boolean remove(K key) throws Exception {
        TableTransaction<K, V> tx = tableStore.startTransaction(orderIdProvider.nextId());
        tx.remove(key);
        tx.commit();
        return true;
    }

    public void remove(Iterable<K> keys) throws Exception {
        TableTransaction<K, V> tx = tableStore.startTransaction(orderIdProvider.nextId());
        for (K key : keys) {
            tx.remove(key);
        }
        tx.commit();
    }

    public void listKeys(ValueStream<K> stream) throws Exception {
        for (K key : tableStore.getImmutableRows().keySet()) {
            if (stream.stream(key) != key) {
                break;
            }
        }
        stream.stream(null); //EOS
    }

    public void listEntries(ValueStream<Entry<K, TimestampedValue<V>>> stream) throws Exception {
        for (Entry<K, TimestampedValue<V>> e : tableStore.getImmutableRows().entrySet()) {
            if (stream.stream(e) != e) {
                break;
            }
        }
        stream.stream(null); //EOS
    }

    public void getMutatedRowsSince(long transationId, TransactionSetStream<K, V> transactionSetStream) throws Exception {
        tableStore.getMutatedRowsSince(transationId, transactionSetStream);
    }

    //  Use for testing
    public boolean compare(AmzaTable<K, V> amzaTable) throws Exception {
        NavigableMap<K, TimestampedValue<V>> immutableRows = amzaTable.tableStore.getImmutableRows();
        int compared = 0;
        for (Entry<K, TimestampedValue<V>> e : immutableRows.entrySet()) {
            compared++;
            TimestampedValue<V> timestampedValue = tableStore.getTimestampedValue(e.getKey());

            String comparing = tableName.getRingName() + ":" + tableName.getTableName()
                    + " to " + amzaTable.tableName.getRingName() + ":" + amzaTable.tableName.getTableName();

            if (timestampedValue == null) {
                System.out.println("INCONSISTENCY: " + comparing + " key:null"
                        + " != " + e.getValue().getTimestamp()
                        + "' -- " + timestampedValue + " vs " + e.getValue());
                return false;
            }
            if (e.getValue().getTimestamp() != timestampedValue.getTimestamp()) {
                System.out.println("INCONSISTENCY: " + comparing + " timstamp:'" + timestampedValue.getTimestamp()
                        + "' != '" + e.getValue().getTimestamp()
                        + "' -- " + timestampedValue + " vs " + e.getValue());
                return false;
            }
            if (e.getValue().getTombstoned() != timestampedValue.getTombstoned()) {
                System.out.println("INCONSISTENCY: " + comparing + " tombstone:" + timestampedValue.getTombstoned()
                        + " != '" + e.getValue().getTombstoned()
                        + "' -- " + timestampedValue + " vs " + e.getValue());
                return false;
            }
            if (e.getValue().getValue() == null && timestampedValue.getValue() != null) {
                System.out.println("INCONSISTENCY: " + comparing + " null values:" + timestampedValue.getTombstoned()
                        + " != '" + e.getValue().getTombstoned()
                        + "' -- " + timestampedValue + " vs " + e.getValue());
                return false;
            }
            if (e.getValue().getValue() != null && !e.getValue().getValue().equals(timestampedValue.getValue())) {
                System.out.println("INCONSISTENCY: " + comparing + " value:'" + timestampedValue.getValue()
                        + "' != '" + e.getValue().getValue()
                        + "' aClass:" + timestampedValue.getValue().getClass()
                        + "' bClass:" + e.getValue().getValue().getClass()
                        + "' -- " + timestampedValue + " vs " + e.getValue());
                return false;
            }
        }
        System.out.println("table:" + amzaTable.tableName.getTableName() + " compared:" + compared + " keys");
        return true;
    }
}