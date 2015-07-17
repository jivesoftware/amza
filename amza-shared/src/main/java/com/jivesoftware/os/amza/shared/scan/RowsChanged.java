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

import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.KeyedTimestampId;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class RowsChanged implements Commitable {

    private final VersionedPartitionName versionedPartitionName;
    private final long oldestApply;
    private final Map<WALKey, WALValue> apply;
    private final List<KeyedTimestampId> remove;
    private final List<KeyedTimestampId> clobber;
    private final long largestCommittedTxId;

    public RowsChanged(VersionedPartitionName versionedPartitionName,
        long oldestApply,
        Map<WALKey, WALValue> apply,
        List<KeyedTimestampId> remove,
        List<KeyedTimestampId> clobber,
        long largestCommittedTxId) {
        this.versionedPartitionName = versionedPartitionName;
        this.oldestApply = oldestApply;
        this.apply = apply;
        this.remove = remove;
        this.clobber = clobber;
        this.largestCommittedTxId = largestCommittedTxId;
    }

    public VersionedPartitionName getVersionedPartitionName() {
        return versionedPartitionName;
    }

    public long getOldestRowTxId() {
        return oldestApply;
    }

    public Map<WALKey, WALValue> getApply() {
        return apply;
    }

    public List<KeyedTimestampId> getRemove() {
        return remove;
    }

    public List<KeyedTimestampId> getClobbered() {
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
    public boolean commitable(Highwaters highwaters, TxKeyValueStream scan) {
        for (Entry<WALKey, WALValue> cell : apply.entrySet()) {
            try {
                WALValue value = cell.getValue();
                if (!scan.row(-1L, cell.getKey().getKey(), value.getValue(), value.getTimestampId(), value.getTombstoned())) {
                    return false;
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Error while streaming entry set.", ex);
            }
        }
        return true;
    }

    public long getLargestCommittedTxId() {
        return largestCommittedTxId;
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

}
