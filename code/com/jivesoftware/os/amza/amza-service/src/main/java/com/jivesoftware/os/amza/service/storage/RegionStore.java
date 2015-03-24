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

import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALValue;

public class RegionStore implements WALScanable {

    private final WALStorage walStorage;
    private final RowChanges rowChanges;

    public RegionStore(WALStorage rowsStorage, RowChanges rowChanges) {
        this.walStorage = rowsStorage;
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

    public void compactTombstone(long removeTombstonedOlderThanTimestampId) throws Exception {
        walStorage.compactTombstone(removeTombstonedOlderThanTimestampId);
    }

    public WALValue get(WALKey key) throws Exception {
        return walStorage.get(key);
    }

    public boolean containsKey(WALKey key) throws Exception {
        return walStorage.containsKey(key);
    }

    public void takeRowUpdatesSince(long transactionId, WALScan rowUpdates) throws Exception {
        walStorage.takeRowUpdatesSince(transactionId, rowUpdates);
    }

    public RowStoreUpdates startTransaction(long timestamp) throws Exception {
        return new RowStoreUpdates(this, new RowsStorageUpdates(walStorage, timestamp));
    }

    public RowsChanged commit(WALScanable rowUpdates) throws Exception {
        RowsChanged updateMap = walStorage.update(rowUpdates);
        if (rowChanges != null && !updateMap.isEmpty()) {
            rowChanges.changes(updateMap);
        }
        return updateMap;
    }

}
