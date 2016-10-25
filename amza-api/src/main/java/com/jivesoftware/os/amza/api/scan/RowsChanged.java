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
package com.jivesoftware.os.amza.api.scan;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.KeyedTimestampId;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALValue;
import java.util.List;
import java.util.Map;

public class RowsChanged {

    private final VersionedPartitionName versionedPartitionName;
    private final Map<WALKey, WALValue> apply;
    private final List<KeyedTimestampId> remove;
    private final List<KeyedTimestampId> clobber;
    private final long smallestCommittedTxId;
    private final long largestCommittedTxId;
    private final int deltaIndex;

    public RowsChanged(VersionedPartitionName versionedPartitionName,
        Map<WALKey, WALValue> apply,
        List<KeyedTimestampId> remove,
        List<KeyedTimestampId> clobber,
        long smallestCommittedTxId,
        long largestCommittedTxId,
        int deltaIndex) {
        this.versionedPartitionName = versionedPartitionName;
        this.apply = apply;
        this.remove = remove;
        this.clobber = clobber;
        this.smallestCommittedTxId = smallestCommittedTxId;
        this.largestCommittedTxId = largestCommittedTxId;
        this.deltaIndex = deltaIndex;
    }

    public VersionedPartitionName getVersionedPartitionName() {
        return versionedPartitionName;
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

    public long getSmallestCommittedTxId() {
        return smallestCommittedTxId;
    }

    public long getLargestCommittedTxId() {
        return largestCommittedTxId;
    }

    public int getDeltaIndex() {
        return deltaIndex;
    }

    @Override
    public String toString() {
        return "RowsChanged{" +
            "versionedPartitionName=" + versionedPartitionName +
            ", apply=" + apply +
            ", remove=" + remove +
            ", clobber=" + clobber +
            ", smallestCommittedTxId=" + smallestCommittedTxId +
            ", largestCommittedTxId=" + largestCommittedTxId +
            ", deltaIndex=" + deltaIndex +
            '}';
    }

}
