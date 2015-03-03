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
package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowScanable;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public class RowUpdatesPayload implements MessagePayload, RowScanable {

    private TableName mapName;
    private long largestTransactionId;
    private List<RowIndexKey> keys;
    private List<RowIndexValue> values;

    /**
     * for serialization
     */
    public RowUpdatesPayload() {
    }

    public RowUpdatesPayload(TableName mapName, long largestTransactionId, List<RowIndexKey> keys, List<RowIndexValue> values) {
        this.mapName = mapName;
        this.largestTransactionId = largestTransactionId;
        this.keys = keys;
        this.values = values;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeObject(mapName, TableName.class);
        output.writeLong(largestTransactionId);
        output.writeObject(keys, List.class);
        output.writeObject(values, List.class);
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
        this.mapName = (TableName) input.readObject(TableName.class);
        this.largestTransactionId = input.readLong();
        this.keys = (List<RowIndexKey>) input.readObject(List.class);
        this.values = (List<RowIndexValue>) input.readObject(List.class);
    }

    public TableName getMapName() {
        return mapName;
    }

    public int size() {
        return keys.size();
    }

    @Override
    public <E extends Exception> void rowScan(RowScan<E> entryStream) throws E {
        for (int i = 0; i < keys.size(); i++) {
            if (!entryStream.row(largestTransactionId, keys.get(i), values.get(i))) {
                return;
            }
        }
    }

    @Override
    public <E extends Exception> void rangeScan(RowIndexKey from, RowIndexKey to, RowScan<E> rowScan) throws E {
        for (int i = 0; i < keys.size(); i++) {
            if (!rowScan.row(largestTransactionId, keys.get(i), values.get(i))) {
                return;
            }
        }
    }

    @Override
    public String toString() {
        return "UpdatesPayload{" + "mapName=" + mapName + ", keys=" + keys + ", values=" + values + '}';
    }

}
