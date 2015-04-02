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

import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;

public class RegionStore implements WALScanable {

    private final AmzaStats amzaStats;
    private final RegionName regionName;
    private final WALStorage walStorage;
    private final RowChanges rowChanges;

    public RegionStore(AmzaStats amzaStats, RegionName regionName, WALStorage walStorage, RowChanges rowChanges) {
        this.amzaStats = amzaStats;
        this.regionName = regionName;
        this.walStorage = walStorage;
        this.rowChanges = rowChanges;
    }

    public void load() throws Exception {
        walStorage.load();
    }

    @Override
    public void rowScan(WALScan walScan) throws Exception {
        walStorage.rowScan(walScan);
    }

    @Override
    public void rangeScan(WALKey from, WALKey to, WALScan walScan) throws Exception {
        walStorage.rangeScan(from, to, walScan);
    }

    public void compactTombstone(long removeTombstonedOlderThanTimestampId, long ttlTimestampId) throws Exception {
        walStorage.compactTombstone(removeTombstonedOlderThanTimestampId, ttlTimestampId);
    }

    public WALValue get(WALKey key) throws Exception {
        return walStorage.get(key);
    }

    public boolean containsKey(WALKey key) throws Exception {
        return walStorage.containsKey(key);
    }

    public void takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
        walStorage.takeRowUpdatesSince(transactionId, rowStream);
    }

    public RowStoreUpdates startTransaction(long timestamp) throws Exception {
        return new RowStoreUpdates(amzaStats, regionName, this, new RowsStorageUpdates(walStorage, timestamp));
    }

    public RowsChanged commit(WALStorageUpdateMode updateMode, WALScanable rowUpdates) throws Exception {
        RowsChanged updateMap = walStorage.update(updateMode, rowUpdates);
        if (rowChanges != null && !updateMap.isEmpty()) {
            rowChanges.changes(updateMap);
        }
        return updateMap;
    }

    public void updatedStorageDescriptor(WALStorageDescriptor storageDescriptor) throws Exception {
        walStorage.updatedStorageDescriptor(storageDescriptor);
    }

    public long size() throws Exception {
        return walStorage.size();
    }
}
