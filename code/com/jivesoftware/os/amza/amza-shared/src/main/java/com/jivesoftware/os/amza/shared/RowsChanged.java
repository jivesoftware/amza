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

public class RowsChanged implements WALScanable {

    private final RegionName regionName;
    private final long oldestApply;
    private final NavigableMap<WALKey, WALValue> apply;
    private final NavigableMap<WALKey, WALValue> remove;
    private final Multimap<WALKey, WALValue> clobber;

    public RowsChanged(RegionName regionName,
        long oldestApply,
        NavigableMap<WALKey, WALValue> apply,
        NavigableMap<WALKey, WALValue> remove,
        Multimap<WALKey, WALValue> clobber) {
        this.regionName = regionName;
        this.oldestApply = oldestApply;
        this.apply = apply;
        this.remove = remove;
        this.clobber = clobber;
    }

    public RegionName getRegionName() {
        return regionName;
    }

    public long getOldestApply() {
        return oldestApply;
    }

    public NavigableMap<WALKey, WALValue> getApply() {
        return apply;
    }

    public NavigableMap<WALKey, WALValue> getRemove() {
        return remove;
    }

    public Multimap<WALKey, WALValue> getClobbered() {
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
    public <E extends Exception> void rowScan(WALScan<E> rowStream) {
        for (Map.Entry<WALKey, WALValue> e : apply.entrySet()) {
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
    public <E extends Exception> void rangeScan(WALKey from, WALKey to, WALScan<E> rowStream) throws E {
        for (Map.Entry<WALKey, WALValue> e : apply.subMap(from, to).entrySet()) {
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
