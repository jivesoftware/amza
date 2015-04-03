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

import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.MemoryWALIndex;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author jonathan.colt
 */
class MultiRegionWALScanBatchinator implements RowStream {

    private final AmzaStats amzaStats;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final RegionProvider regionProvider;
    private final Map<RegionName, RegionBatch> regionBatches = new HashMap<>();

    public MultiRegionWALScanBatchinator(AmzaStats amzaStats, RowMarshaller<byte[]> rowMarshaller, RegionProvider regionProvider) {
        this.amzaStats = amzaStats;
        this.rowMarshaller = rowMarshaller;
        this.regionProvider = regionProvider;
    }

    @Override
    public boolean row(long rowFP, long rowTxId, byte rowType, byte[] rawRow) throws Exception {
        RowMarshaller.WALRow row = rowMarshaller.fromRow(rawRow);
        ByteBuffer bb = ByteBuffer.wrap(row.getKey().getKey());
        byte[] regionNameBytes = new byte[bb.getShort()];
        bb.get(regionNameBytes);
        byte[] keyBytes = new byte[bb.getInt()];
        bb.get(keyBytes);

        RegionName regionName = RegionName.fromBytes(regionNameBytes);
        RegionBatch regionBatch = regionBatches.get(regionName);
        if (regionBatch == null) {
            RegionStore regionStore = regionProvider.getRegionStore(regionName);
            if (regionStore == null) {
                throw new RuntimeException("Encountered an undefined region." + regionName);
            }
            regionBatch = new RegionBatch(amzaStats, regionName, regionStore);
            regionBatches.put(regionName, regionBatch);
        }
        regionBatch.add(rowTxId, new WALKey(keyBytes), row.getValue());
        return true;
    }

    public boolean flush() throws Exception {
        boolean flushed = false;
        for (Map.Entry<RegionName, RegionBatch> entrySet : regionBatches.entrySet()) {
            RegionBatch regionBatch = entrySet.getValue();
            flushed |= regionBatch.flush();

        }
        return flushed;
    }

    static class RegionBatch {

        private final AmzaStats amzaStats;
        private final RegionName regionName;
        private final RegionStore regionStore;
        private final TreeMap<WALKey, WALValue> batch = new TreeMap<>();
        private final MutableLong lastTxId;
        private final MutableBoolean flushed = new MutableBoolean(false);

        public RegionBatch(AmzaStats amzaStats, RegionName regionName, RegionStore regionStore) {
            this.amzaStats = amzaStats;
            this.regionName = regionName;
            this.regionStore = regionStore;
            this.lastTxId = new MutableLong(Long.MIN_VALUE);
        }

        public void add(long rowTxId, WALKey key, WALValue value) throws Exception {
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
                lastTxId.setValue(rowTxId);
            } else if (lastTxId.longValue() != rowTxId) {
                lastTxId.setValue(rowTxId);
                flush();
            }
        }

        public boolean flush() throws Exception {
            if (!batch.isEmpty()) {
                RowsChanged changes = regionStore.commit(WALStorageUpdateMode.noReplication, new MemoryWALIndex(batch));
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

}
