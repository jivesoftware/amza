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
package com.jivesoftware.os.amza.shared;

import com.google.common.collect.Multimap;
import java.util.Map;
import java.util.NavigableMap;

public class RowsChanged implements RowScanable {

    private final TableName tableName;
    private final NavigableMap<RowIndexKey, RowIndexValue> apply;
    private final NavigableMap<RowIndexKey, RowIndexValue> remove;
    private final Multimap<RowIndexKey, RowIndexValue> clobber;

    public RowsChanged(TableName tableName,
            NavigableMap<RowIndexKey, RowIndexValue> apply,
            NavigableMap<RowIndexKey, RowIndexValue> remove,
            Multimap<RowIndexKey, RowIndexValue> clobber) {
        this.tableName = tableName;
        this.apply = apply;
        this.remove = remove;
        this.clobber = clobber;
    }

    public TableName getTableName() {
        return tableName;
    }

    public NavigableMap<RowIndexKey, RowIndexValue> getApply() {
        return apply;
    }

    public NavigableMap<RowIndexKey, RowIndexValue> getRemove() {
        return remove;
    }

    public Multimap<RowIndexKey, RowIndexValue> getClobbered() {
        return clobber;
    }

    public boolean isEmpty() {
        if (apply != null && !apply.isEmpty()) {
            return false;
        }
        if (remove != null && !remove.isEmpty()) {
            return false;
        }
        if (clobber != null && !clobber.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public <E extends Exception> void rowScan(RowScan<E> rowStream) {
        for (Map.Entry<RowIndexKey, RowIndexValue> e : apply.entrySet()) {
            try {
                if (!rowStream.row(-1, e.getKey(), e.getValue())) {
                    break;
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Error while streaming entry set.", ex);
            }
        }
    }

    @Override
    public <E extends Exception> void rangeScan(RowIndexKey from, RowIndexKey to, RowScan<E> rowStream) throws E {
        for (Map.Entry<RowIndexKey, RowIndexValue> e : apply.subMap(from, to).entrySet()) {
            try {
                if (!rowStream.row(-1, e.getKey(), e.getValue())) {
                    break;
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Error while streaming entry set.", ex);
            }
        }
    }
}