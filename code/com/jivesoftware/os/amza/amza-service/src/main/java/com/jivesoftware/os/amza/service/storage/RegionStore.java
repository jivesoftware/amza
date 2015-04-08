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
package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.service.storage.delta.DeltaWALStorage;
import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;

public class RegionStore implements RangeScannable<WALValue> {

    private final AmzaStats amzaStats;
    private final RegionName regionName;
    private final DeltaWALStorage deltaWALStorage;
    private final WALStorage walStorage;
    private final RowChanges rowChanges;

    public RegionStore(AmzaStats amzaStats,
        RegionName regionName,
        DeltaWALStorage deltaWALStorage,
        WALStorage walStorage,
        RowChanges rowChanges) {

        this.amzaStats = amzaStats;
        this.regionName = regionName;
        this.deltaWALStorage = deltaWALStorage;
        this.walStorage = walStorage;
        this.rowChanges = rowChanges;
    }

    public WALStorage getWalStorage() {
        return walStorage;
    }

    public void load() throws Exception {
        walStorage.load();
    }

    @Override
    public void rowScan(Scan<WALValue> scan) throws Exception {
        deltaWALStorage.rowScan(regionName, walStorage, scan);
    }

    @Override
    public void rangeScan(WALKey from, WALKey to, Scan<WALValue> scan) throws Exception {
        deltaWALStorage.rangeScan(regionName, walStorage, from, to, scan);
    }

    public void compactTombstone(long removeTombstonedOlderThanTimestampId, long ttlTimestampId) throws Exception {
        walStorage.compactTombstone(removeTombstonedOlderThanTimestampId, ttlTimestampId);
    }

    public WALValue get(WALKey key) throws Exception {
        return deltaWALStorage.get(regionName, walStorage, key);
    }

    public boolean containsKey(WALKey key) throws Exception {
        return deltaWALStorage.containsKey(regionName, walStorage, key);
    }

    public void takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
        deltaWALStorage.takeRowUpdatesSince(regionName, walStorage, transactionId, rowStream);
    }

    public RowStoreUpdates startTransaction(long timestamp) throws Exception {
        return new RowStoreUpdates(amzaStats, regionName, this, new RowsStorageUpdates(walStorage, timestamp));
    }

    public RowsChanged commit(WALStorageUpdateMode updateMode, Scannable<WALValue> rowUpdates) throws Exception {
        RowsChanged updateMap = deltaWALStorage.update(regionName, walStorage, updateMode, rowUpdates);
        if (rowChanges != null && !updateMap.isEmpty()) {
            rowChanges.changes(updateMap);
        }
        return updateMap;
    }

    public void directCommit(long txId, Scannable<WALValue> scanable) throws Exception {
        walStorage.update(txId, WALStorageUpdateMode.noReplication, scanable);
    }

    public void updatedStorageDescriptor(WALStorageDescriptor storageDescriptor) throws Exception {
        walStorage.updatedStorageDescriptor(storageDescriptor);
    }

    public long count() throws Exception {
        return deltaWALStorage.count(regionName, walStorage);
    }

}
