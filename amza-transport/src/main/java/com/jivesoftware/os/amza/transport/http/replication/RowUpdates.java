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
package com.jivesoftware.os.amza.transport.http.replication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALRow;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import java.util.List;

public class RowUpdates {

    private final long highestTransactionId;
    private final PartitionName partitionName;
    private final List<Long> rowTxIds;
    private final List<byte[]> rows;

    @JsonCreator
    public RowUpdates(
        @JsonProperty("highestTransactionId") long highestTransactionId,
        @JsonProperty("tableName") PartitionName partitionName,
        @JsonProperty("rowTxIds") List<Long> rowTxIds,
        @JsonProperty("rows") List<byte[]> rows) {
        this.highestTransactionId = highestTransactionId;
        this.partitionName = partitionName;
        this.rowTxIds = rowTxIds;
        this.rows = rows;
    }

    public long getHighestTransactionId() {
        return highestTransactionId;
    }

    public PartitionName getPartitionName() {
        return partitionName;
    }

    public List<Long> getRowTxIds() {
        return rowTxIds;
    }

    public List<byte[]> getRows() {
        return rows;
    }

    public void stream(PrimaryRowMarshaller<byte[]> marshaller, Scan<WALValue> scan) throws Exception {
        for (int i = 0; i < rowTxIds.size(); i++) {
            WALRow row = marshaller.fromRow(rows.get(i));
            if (!scan.row(rowTxIds.get(i), row.key, row.value)) {
                break;
            }
        }
    }

    @Override
    public String toString() {
        return "RowUpdates{"
            + "highestTransactionId=" + highestTransactionId
            + ", partitionName=" + partitionName
            + ", highestTransactionId=" + highestTransactionId
            + ", changes=" + rows + '}';
    }
}
