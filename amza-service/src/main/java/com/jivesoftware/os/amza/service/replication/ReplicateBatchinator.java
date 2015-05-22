/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowType;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.storage.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.storage.WALRow;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author jonathan.colt
 */
class ReplicateBatchinator implements RowStream {

    private final PrimaryRowMarshaller<byte[]> rowMarshaller;
    private final RegionName regionName;
    private final AmzaRegionChangeReplicator replicator;
    private final Map<WALKey, WALValue> batch = new HashMap<>();
    private final MutableLong lastTxId;
    private final MutableBoolean flushed = new MutableBoolean(false);

    public ReplicateBatchinator(PrimaryRowMarshaller<byte[]> rowMarshaller, RegionName regionName, AmzaRegionChangeReplicator replicator) {
        this.rowMarshaller = rowMarshaller;
        this.regionName = regionName;
        this.replicator = replicator;
        this.lastTxId = new MutableLong(Long.MIN_VALUE);
    }

    @Override
    public boolean row(long rowFP, long rowTxId, RowType rowType, byte[] rawRow) throws Exception {
        if (rowType == RowType.primary) {
            flushed.setValue(true);
            WALRow row = rowMarshaller.fromRow(rawRow);
            WALKey key = row.key;
            WALValue value = row.value;
            WALValue got = batch.get(key);
            if (got == null) {
                batch.put(key, value);
            } else {
                if (got.getTimestampId() < value.getTimestampId()) {
                    batch.put(key, value);
                }
            }
            if (lastTxId.longValue() == Long.MIN_VALUE) {
                lastTxId.setValue(rowTxId);
            } else if (lastTxId.longValue() != rowTxId) {
                lastTxId.setValue(rowTxId);
                flush();
            }
        }
        return true;
    }

    public boolean flush() throws Exception {
        if (!batch.isEmpty()) {
            if (replicator.replicateLocalUpdates(regionName, new MemoryWALUpdates(batch, null), false).get()) {
                batch.clear();
            }
        }
        return flushed.booleanValue();
    }

}
