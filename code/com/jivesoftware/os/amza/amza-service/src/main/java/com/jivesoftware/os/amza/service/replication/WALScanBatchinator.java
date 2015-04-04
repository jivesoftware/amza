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

import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import java.util.TreeMap;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author jonathan.colt
 */
class WALScanBatchinator implements RowStream {

    private final AmzaStats amzaStats;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final RegionName regionName;
    private final RegionStore regionStore;
    private final TreeMap<WALKey, WALValue> batch = new TreeMap<>();
    private final MutableLong lastTxId;
    private final MutableBoolean flushed = new MutableBoolean(false);

    public WALScanBatchinator(AmzaStats amzaStats, RowMarshaller<byte[]> rowMarshaller, RegionName regionName, RegionStore regionStore) {
        this.amzaStats = amzaStats;
        this.rowMarshaller = rowMarshaller;
        this.regionName = regionName;
        this.regionStore = regionStore;
        this.lastTxId = new MutableLong(Long.MIN_VALUE);
    }

    @Override
    public boolean row(long rowFP, long rowTxId, byte rowType, byte[] rawRow) throws Exception {
        flushed.setValue(true);
        RowMarshaller.WALRow row = rowMarshaller.fromRow(rawRow);
        WALValue got = batch.get(row.getKey());
        if (got == null) {
            batch.put(row.getKey(), row.getValue());
        } else {
            if (got.getTimestampId() < row.getValue().getTimestampId()) {
                batch.put(row.getKey(), row.getValue());
            }
        }
        if (lastTxId.longValue() == Long.MIN_VALUE) {
            lastTxId.setValue(rowTxId);
        } else if (lastTxId.longValue() != rowTxId) {
            lastTxId.setValue(rowTxId);
            flush();
        }
        return true;
    }

    public boolean flush() throws Exception {
        if (!batch.isEmpty()) {
            RowsChanged changes = regionStore.commit(WALStorageUpdateMode.noReplication, new MemoryWALUpdates(batch));
            amzaStats.receivedApplied(regionName, changes.getApply().size(), changes.getOldestRowTxId());
            batch.clear();
        }
        if (!flushed.booleanValue()) {
            amzaStats.received(regionName, 0, Long.MAX_VALUE);
            amzaStats.receivedApplied(regionName, 0, Long.MAX_VALUE);
        }
        return flushed.booleanValue();
    }

}
