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

import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.storage.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.storage.WALRow;

public class BinaryPrimaryRowMarshaller implements PrimaryRowMarshaller<byte[]> {

    @Override
    public byte[] toRow(WALKey k, WALValue v) throws Exception {
        HeapFiler filer = new HeapFiler();
        UIO.writeByteArray(filer, v.getValue(), "value");
        UIO.writeLong(filer, v.getTimestampId(), "timestamp");
        UIO.writeBoolean(filer, v.getTombstoned(), "tombstone");
        UIO.writeByteArray(filer, k.getKey(), "key");
        return filer.getBytes();

    }

    @Override
    public WALRow fromRow(byte[] row) throws Exception {
        HeapFiler filer = new HeapFiler(row);
        final byte[] value = UIO.readByteArray(filer, "value");
        final long timestamp = UIO.readLong(filer, "timestamp");
        final boolean tombstone = UIO.readBoolean(filer, "tombstone");
        final byte[] key = UIO.readByteArray(filer, "key");
        return new WALRow(new WALKey(key), new WALValue(value, timestamp, tombstone));
    }

    @Override
    public byte[] valueFromRow(byte[] row) throws Exception {
        HeapFiler filer = new HeapFiler(row);
        return UIO.readByteArray(filer, "value");
    }
}
