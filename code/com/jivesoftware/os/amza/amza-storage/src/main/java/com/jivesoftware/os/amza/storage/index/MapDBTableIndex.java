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
package com.jivesoftware.os.amza.storage.index;

import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class MapDBTableIndex<K, V> implements TableIndex<K, V> {

    private final DB db;
    private final BTreeMap<K, TimestampedValue<V>> treeMap;

    public MapDBTableIndex(String mapName) {
        db = DBMaker.newDirectMemoryDB()
            .closeOnJvmShutdown()
            .make();
        treeMap = db.getTreeMap(mapName);
    }

    @Override
    public void flush() {
        db.commit();
    }

    @Override
    public Entry<K, TimestampedValue<V>> lowerEntry(K key) {
        return treeMap.lowerEntry(key);
    }

    @Override
    public K lowerKey(K key) {
        return treeMap.lowerKey(key);
    }

    @Override
    public Entry<K, TimestampedValue<V>> floorEntry(K key) {
        return treeMap.floorEntry(key);
    }

    @Override
    public K floorKey(K key) {
        return treeMap.floorKey(key);
    }

    @Override
    public Entry<K, TimestampedValue<V>> ceilingEntry(K key) {
        return treeMap.ceilingEntry(key);
    }

    @Override
    public K ceilingKey(K key) {
        return treeMap.ceilingKey(key);
    }

    @Override
    public Entry<K, TimestampedValue<V>> higherEntry(K key) {
        return treeMap.higherEntry(key);
    }

    @Override
    public K higherKey(K key) {
        return treeMap.higherKey(key);
    }

    @Override
    public Entry<K, TimestampedValue<V>> firstEntry() {
        return treeMap.firstEntry();
    }

    @Override
    public Entry<K, TimestampedValue<V>> lastEntry() {
        return treeMap.lastEntry();
    }

    @Override
    public Entry<K, TimestampedValue<V>> pollFirstEntry() {
        return treeMap.pollFirstEntry();
    }

    @Override
    public Entry<K, TimestampedValue<V>> pollLastEntry() {
        return treeMap.pollLastEntry();
    }

    @Override
    public NavigableMap<K, TimestampedValue<V>> descendingMap() {
        return treeMap.descendingMap();
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return treeMap.navigableKeySet();
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return treeMap.descendingKeySet();
    }

    @Override
    public NavigableMap<K, TimestampedValue<V>> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return treeMap.subMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public NavigableMap<K, TimestampedValue<V>> headMap(K toKey, boolean inclusive) {
        return treeMap.headMap(toKey, inclusive);
    }

    @Override
    public NavigableMap<K, TimestampedValue<V>> tailMap(K fromKey, boolean inclusive) {
        return treeMap.tailMap(fromKey, inclusive);
    }

    @Override
    public SortedMap<K, TimestampedValue<V>> subMap(K fromKey, K toKey) {
        return treeMap.subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<K, TimestampedValue<V>> headMap(K toKey) {
        return treeMap.headMap(toKey);
    }

    @Override
    public SortedMap<K, TimestampedValue<V>> tailMap(K fromKey) {
        return treeMap.tailMap(fromKey);
    }

    @Override
    public Comparator<? super K> comparator() {
        return treeMap.comparator();
    }

    @Override
    public K firstKey() {
        return treeMap.firstKey();
    }

    @Override
    public K lastKey() {
        return treeMap.lastKey();
    }

    @Override
    public Set<K> keySet() {
        return treeMap.keySet();
    }

    @Override
    public Collection<TimestampedValue<V>> values() {
        return treeMap.values();
    }

    @Override
    public Set<Entry<K, TimestampedValue<V>>> entrySet() {
        return treeMap.entrySet();
    }

    @Override
    public int size() {
        return treeMap.size();
    }

    @Override
    public boolean isEmpty() {
        return treeMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return treeMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return treeMap.containsValue(value);
    }

    @Override
    public TimestampedValue<V> get(Object key) {
        return treeMap.get(key);
    }

    @Override
    public TimestampedValue<V> put(K key,
        TimestampedValue<V> value) {
        return treeMap.put(key, value);
    }

    @Override
    public TimestampedValue<V> remove(Object key) {
        return treeMap.remove(key);
    }

    @Override
    public void putAll(
        Map<? extends K, ? extends TimestampedValue<V>> m) {
        treeMap.putAll(m);
    }

    @Override
    public void clear() {
        treeMap.clear();
    }
}