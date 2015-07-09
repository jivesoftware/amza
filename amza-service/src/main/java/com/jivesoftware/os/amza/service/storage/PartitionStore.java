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
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.wal.WALValue;

public class PartitionStore implements RangeScannable {

    private final WALStorage walStorage;
    private final boolean hardFlush;

    public PartitionStore(WALStorage walStorage,
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
    public boolean rowScan(KeyValueStream txKeyValueStream) throws Exception {
        return walStorage.rowScan(txKeyValueStream);
    }

    @Override
    public boolean rangeScan(WALKey from, WALKey to, KeyValueStream txKeyValueStream) throws Exception {
        return walStorage.rangeScan(from, to, txKeyValueStream);
    }

    public boolean compactableTombstone(long removeTombstonedOlderTimestampId, long ttlTimestampId) throws Exception {
        return walStorage.compactableTombstone(removeTombstonedOlderTimestampId, ttlTimestampId);
    }

    public void compactTombstone(long removeTombstonedOlderThanTimestampId, long ttlTimestampId) throws Exception {
        walStorage.compactTombstone(removeTombstonedOlderThanTimestampId, ttlTimestampId);
    }

    public WALValue get(byte[] key) throws Exception {
        return walStorage.get(key);
    }

    // TODO keyValues sucks need Keys and KeyStream
    public boolean get(WALKeys keys, KeyValueStream stream) throws Exception {
        return walStorage.get(keys, stream);
    }

    public boolean containsKey(WALKey key) throws Exception {
        return walStorage.containsKey(key);
    }

    public void takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
        walStorage.takeRowUpdatesSince(transactionId, rowStream);
    }

    public RowsChanged directCommit(boolean useUpdateTxId, Commitable updates) throws Exception {
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
