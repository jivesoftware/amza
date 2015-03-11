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
package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.filer.MemoryFiler;
import com.jivesoftware.os.amza.storage.filer.UIO;

public class BinaryRowMarshaller implements RowMarshaller<byte[]> {

    @Override
    public byte[] toRow(long transactionId, RowIndexKey k, RowIndexValue v) throws Exception {
        MemoryFiler filer = new MemoryFiler();
        UIO.writeLong(filer, transactionId, "transactionId");
        UIO.writeByteArray(filer, v.getValue(), "value");
        UIO.writeLong(filer, v.getTimestampId(), "timestamp");
        UIO.writeBoolean(filer, v.getTombstoned(), "tombstone");
        UIO.writeByteArray(filer, k.getKey(), "key");
        return filer.getBytes();

    }

    @Override
    public boolean fromRow(byte[] row, RowScan rowStream) throws Exception {
        MemoryFiler filer = new MemoryFiler(row);
        long transactionId = UIO.readLong(filer, "transactionId");
        byte[] value = UIO.readByteArray(filer, "value");
        long timestamp = UIO.readLong(filer, "timestamp");
        boolean tombstone = UIO.readBoolean(filer, "tombstone");
        byte[] key = UIO.readByteArray(filer, "key");
        return rowStream.row(transactionId, new RowIndexKey(key), new RowIndexValue(value, timestamp, tombstone));
    }

    @Override
    public byte[] valueFromRow(byte[] row) throws Exception {
        MemoryFiler filer = new MemoryFiler(row);
        UIO.readLong(filer, "transactionId");
        return UIO.readByteArray(filer, "value");
    }
}
