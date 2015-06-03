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
package com.jivesoftware.os.amza.shared.scan;

import com.google.common.collect.Table;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALTimestampId;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import java.util.Map;

public class RowsChanged implements Commitable<WALValue> {

    private final VersionedPartitionName versionedPartitionName;
    private final long oldestApply;
    private final Table<Long, WALKey, WALValue> apply;
    private final Map<WALKey, WALTimestampId> remove;
    private final Map<WALKey, WALTimestampId> clobber;

    public RowsChanged(VersionedPartitionName versionedPartitionName,
        long oldestApply,
        Table<Long, WALKey, WALValue> apply,
        Map<WALKey, WALTimestampId> remove,
        Map<WALKey, WALTimestampId> clobber) {
        this.versionedPartitionName = versionedPartitionName;
        this.oldestApply = oldestApply;
        this.apply = apply;
        this.remove = remove;
        this.clobber = clobber;
    }

    public VersionedPartitionName getVersionedPartitionName() {
        return versionedPartitionName;
    }
    
    public long getOldestRowTxId() {
        return oldestApply;
    }

    public Table<Long, WALKey, WALValue> getApply() {
        return apply;
    }

    public Map<WALKey, WALTimestampId> getRemove() {
        return remove;
    }

    public Map<WALKey, WALTimestampId> getClobbered() {
        return clobber;
    }

    public boolean isEmpty() {
        if (apply != null && !apply.isEmpty()) {
            return false;
        }
        if (remove != null && !remove.isEmpty()) {
            return false;
        }
        return !(clobber != null && !clobber.isEmpty());
    }

    @Override
    public void commitable(Highwaters highwaters, Scan<WALValue> scan) {
        for (Table.Cell<Long, WALKey, WALValue> cell : apply.cellSet()) {
            try {
                if (!scan.row(cell.getRowKey(), cell.getColumnKey(), cell.getValue())) {
                    break;
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Error while streaming entry set.", ex);
            }
        }
    }

    @Override
    public String toString() {
        return "RowsChanged{"
            + "versionedPartitionName=" + versionedPartitionName
            + ", oldestApply=" + oldestApply
            + ", apply=" + apply
            + ", remove=" + remove
            + ", clobber=" + clobber + '}';
    }

    public long getLargestCommitedTxId() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
