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
import com.jivesoftware.os.amza.shared.MemoryWALIndex;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALStorageUpateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import java.util.TreeMap;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author jonathan.colt
 */
class WALScanBatchinator implements WALScan {

    private final AmzaStats amzaStats;
    private final RegionName regionName;
    private final RegionStore regionStore;
    private final TreeMap<WALKey, WALValue> batch = new TreeMap<>();
    private final MutableLong lastTxId;
    private final MutableBoolean flushed = new MutableBoolean(false);

    public WALScanBatchinator(AmzaStats amzaStats, RegionName regionName, RegionStore regionStore) {
        this.amzaStats = amzaStats;
        this.regionName = regionName;
        this.regionStore = regionStore;
        this.lastTxId = new MutableLong(Long.MIN_VALUE);
    }

    @Override
    public boolean row(long txId, WALKey key, WALValue value) throws Exception {
        flushed.setValue(true);
        WALValue got = batch.get(key);
        if (got == null) {
            batch.put(key, value);
        } else {
            if (got.getTimestampId() < value.getTimestampId()) {
                batch.put(key, value);
            }
        }
        if (lastTxId.longValue() == Long.MIN_VALUE) {
            lastTxId.setValue(txId);
        } else if (lastTxId.longValue() != txId) {
            lastTxId.setValue(txId);
            flush();
        }
        return true;
    }

    public boolean flush() throws Exception {
        if (!batch.isEmpty()) {
            RowsChanged changes = regionStore.commit(WALStorageUpateMode.updateThenReplicate, new MemoryWALIndex(batch));
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
