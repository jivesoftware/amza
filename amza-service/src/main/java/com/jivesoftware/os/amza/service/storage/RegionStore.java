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

import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RangeScannable;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorage;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.wal.WALValue;

public class RegionStore implements RangeScannable<WALValue> {

    private final WALStorage walStorage;
    private final boolean hardFlush;

    public RegionStore(WALStorage walStorage,
        boolean hardFlush) {

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

    public WALValue[] get(WALKey[] keys) throws Exception {
        return walStorage.get(keys);
    }

    public boolean containsKey(WALKey key) throws Exception {
        return walStorage.containsKey(key);
    }

    public void takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
        walStorage.takeRowUpdatesSince(transactionId, rowStream);
    }

    public RowsChanged directCommit(boolean useUpdateTxId, Commitable<WALValue> updates) throws Exception {
        RowsChanged changes = walStorage.update(useUpdateTxId, updates);
        walStorage.flush(hardFlush);
        return changes;
    }

    public void updatedStorageDescriptor(WALStorageDescriptor storageDescriptor) throws Exception {
        walStorage.updatedStorageDescriptor(storageDescriptor);
    }

    public long count() throws Exception {
        return walStorage.count();
    }

    public long highestTxId() {
        return walStorage.highestTxId();
    }

}
