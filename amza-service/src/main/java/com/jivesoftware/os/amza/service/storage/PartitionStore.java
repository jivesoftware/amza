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

import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.scan.RangeScannable;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;

public class PartitionStore implements RangeScannable {

    private PartitionProperties properties;
    private final WALStorage walStorage;
    private final boolean hardFlush;

    public PartitionStore(PartitionProperties properties,
        WALStorage walStorage,
        boolean hardFlush) {
        this.properties = properties;
        this.walStorage = walStorage;
        this.hardFlush = hardFlush;
    }

    public PartitionProperties getProperties() {
        return properties;
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
    public boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, KeyValueStream txKeyValueStream) throws Exception {
        return walStorage.rangeScan(fromPrefix, fromKey, toPrefix, toKey, txKeyValueStream);
    }

    public boolean compactableTombstone(long removeTombstonedOlderTimestampId, long ttlTimestampId) throws Exception {
        return walStorage.compactableTombstone(removeTombstonedOlderTimestampId, ttlTimestampId);
    }

    public void compactTombstone(long removeTombstonedOlderThanTimestampId, long ttlTimestampId, boolean force) throws Exception {
        walStorage.compactTombstone(properties.rowType, removeTombstonedOlderThanTimestampId, ttlTimestampId, force);
    }

    public TimestampedValue getTimestampedValue(byte[] prefix, byte[] key) throws Exception {
        return walStorage.getTimestampedValue(prefix, key);
    }

    public boolean streamValues(byte[] prefix, UnprefixedWALKeys keys, KeyValueStream stream) throws Exception {
        return walStorage.streamValues(prefix, keys, stream);
    }

    public boolean containsKey(byte[] prefix, byte[] key) throws Exception {
        boolean[] result = new boolean[1];
        walStorage.containsKeys(prefix, stream -> stream.stream(key), (_prefix, _key, contained) -> {
            result[0] = contained;
            return true;
        });
        return result[0];
    }

    public boolean containsKeys(byte[] prefix, UnprefixedWALKeys keys, KeyContainedStream stream) throws Exception {
        return walStorage.containsKeys(prefix, keys, stream);
    }

    public void takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
        walStorage.takeRowUpdatesSince(transactionId, rowStream);
    }

    public RowsChanged merge(PartitionProperties partitionProperties, long forceTxId, byte[] prefix, Commitable updates) throws Exception {
        RowsChanged changes = walStorage.update(partitionProperties.rowType, forceTxId, true, prefix, updates);
        walStorage.flush(hardFlush);
        return changes;
    }

    public void updateProperties(PartitionProperties properties) throws Exception {
        this.properties = properties;
        walStorage.updatedStorageDescriptor(properties.walStorageDescriptor);
    }

    public long highestTxId() {
        return walStorage.highestTxId();
    }

}
