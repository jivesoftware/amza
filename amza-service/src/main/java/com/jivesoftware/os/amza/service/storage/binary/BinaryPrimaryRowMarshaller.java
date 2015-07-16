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
package com.jivesoftware.os.amza.service.storage.binary;

import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.scan.TxKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;

public class BinaryPrimaryRowMarshaller implements PrimaryRowMarshaller<byte[]> {

    @Override
    public byte[] toRow(byte[] key, byte[] value, long timestamp, boolean tombstoned) throws Exception {
        HeapFiler filer = new HeapFiler();
        UIO.writeLong(filer, timestamp, "timestamp");
        UIO.writeBoolean(filer, tombstoned, "tombstone");
        UIO.writeByteArray(filer, value, "value");
        UIO.writeByteArray(filer, key, "key");
        return filer.getBytes();
    }

    @Override
    public boolean fromRow(byte[] row, KeyValueStream keyValueStream) throws Exception {
        HeapFiler filer = new HeapFiler(row);
        long timestamp = UIO.readLong(filer, "timestamp");
        boolean tombstone = UIO.readBoolean(filer, "tombstone");
        byte[] value = UIO.readByteArray(filer, "value");
        byte[] key = UIO.readByteArray(filer, "key");
        return keyValueStream.stream(key, value, timestamp, tombstone);
    }

    @Override
    public boolean fromRow(byte[] row, long txId, TxKeyValueStream keyValueStream) throws Exception {
        HeapFiler filer = new HeapFiler(row);
        long timestamp = UIO.readLong(filer, "timestamp");
        boolean tombstone = UIO.readBoolean(filer, "tombstone");
        byte[] value = UIO.readByteArray(filer, "value");
        byte[] key = UIO.readByteArray(filer, "key");
        return keyValueStream.row(txId, key, value, timestamp, tombstone);
    }

    @Override
    public byte[] valueFromRow(byte[] row) throws Exception {
        HeapFiler filer = new HeapFiler(row);
        filer.seek(8 + 1);
        return UIO.readByteArray(filer, "value");
    }

    @Override
    public long timestampFromRow(byte[] row) throws Exception {
        return UIO.bytesLong(row, 0);
    }
}
