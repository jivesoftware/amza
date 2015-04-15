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

import com.jivesoftware.os.amza.shared.RangeScannable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;

public class RegionStore implements RangeScannable<WALValue> {

    private final AmzaStats amzaStats;
    private final RegionName regionName;
    private final WALStorage walStorage;
    private final boolean hardFlush;

    public RegionStore(AmzaStats amzaStats,
        RegionName regionName,
        WALStorage walStorage,
        boolean hardFlush) {

        this.amzaStats = amzaStats;
        this.regionName = regionName;
        this.walStorage = walStorage;
        this.hardFlush = hardFlush;
    }

    public WALStorage getWalStorage() {
        return walStorage;
    }

    public void load() throws Exception {
        walStorage.load();
    }

    public void flush(boolean fsync) throws Exception {
        walStorage.flush(fsync);
    }

    @Override
    public void rowScan(Scan<WALValue> scan) throws Exception {
        walStorage.rowScan(scan);
    }

    @Override
    public void rangeScan(WALKey from, WALKey to, Scan<WALValue> scan) throws Exception {
        walStorage.rangeScan(from, to, scan);
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

    public RowsChanged directCommit(Long txId, WALReplicator replicator, WALStorageUpdateMode mode, Scannable<WALValue> updates) throws Exception {
        RowsChanged changes = walStorage.update(txId, replicator, mode, updates);
        walStorage.flush(hardFlush);
        return changes;
    }

    public void updatedStorageDescriptor(WALStorageDescriptor storageDescriptor) throws Exception {
        walStorage.updatedStorageDescriptor(storageDescriptor);
    }

    public long count() throws Exception {
        return walStorage.count();
    }

}
