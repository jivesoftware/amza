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

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALKey;

public class BinaryPrimaryRowMarshaller implements PrimaryRowMarshaller<byte[]> {

    @Override
    public byte[] toRow(byte[] pk, byte[] value, long timestamp, boolean tombstoned, long version) throws Exception {
        HeapFiler filer = new HeapFiler(new byte[8 + 1 + 8 + 4 + (value != null ? value.length : 0) + 4 + pk.length]);
        UIO.writeLong(filer, timestamp, "timestamp");
        UIO.writeBoolean(filer, tombstoned, "tombstone");
        UIO.writeLong(filer, version, "version");
        UIO.writeByteArray(filer, value, "value");
        UIO.writeByteArray(filer, pk, "key");
        return filer.getBytes();
    }

    @Override
    public int sizeInBytes(int pkSizeInBytes, int valueSizeInBytes) {
        return 8 + 1 + 8 + 4 + valueSizeInBytes + 4 + pkSizeInBytes;
    }

    @Override
    public boolean fromRows(FpRows fpRows, FpKeyValueStream fpKeyValueStream) throws Exception {
        return WALKey.decompose(
            stream -> fpRows.consume((fp, row) -> {
                HeapFiler filer = new HeapFiler(row);
                long timestamp = UIO.readLong(filer, "timestamp");
                boolean tombstone = UIO.readBoolean(filer, "tombstone");
                long version = UIO.readLong(filer, "version");
                byte[] value = UIO.readByteArray(filer, "value");
                byte[] key = UIO.readByteArray(filer, "key");
                return stream.stream(-1, fp, key, value, timestamp, tombstone, version, row);
            }),
            (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row)
            -> fpKeyValueStream.stream(fp, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion));
    }

    @Override
    public boolean fromRows(TxFpRows txFpRows, TxKeyValueStream txKeyValueStream) throws Exception {
        return WALKey.decompose(
            stream -> txFpRows.consume((txId, fp, row) -> {
                HeapFiler filer = new HeapFiler(row);
                long timestamp = UIO.readLong(filer, "timestamp");
                boolean tombstone = UIO.readBoolean(filer, "tombstone");
                long version = UIO.readLong(filer, "version");
                byte[] value = UIO.readByteArray(filer, "value");
                byte[] key = UIO.readByteArray(filer, "key");
                return stream.stream(txId, fp, key, value, timestamp, tombstone, version, row);
            }),
            (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row)
            -> txKeyValueStream.stream(txId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion));
    }

    @Override
    public boolean fromRows(TxFpRows txFpRows, UnprefixedTxKeyValueStream txKeyValueStream) throws Exception {
        return WALKey.decompose(
            stream -> txFpRows.consume((txId, fp, row) -> {
                HeapFiler filer = new HeapFiler(row);
                long timestamp = UIO.readLong(filer, "timestamp");
                boolean tombstone = UIO.readBoolean(filer, "tombstone");
                long version = UIO.readLong(filer, "version");
                byte[] value = UIO.readByteArray(filer, "value");
                byte[] key = UIO.readByteArray(filer, "key");
                return stream.stream(txId, fp, key, value, timestamp, tombstone, version, row);
            }),
            (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row)
            -> txKeyValueStream.row(txId, key, value, valueTimestamp, valueTombstoned, valueVersion));
    }

    @Override
    public boolean fromRows(TxFpRows txFpRows, WALKey.TxFpKeyValueEntryStream<byte[]> txFpKeyValueStream) throws Exception {
        return WALKey.decompose(
            txFpRawKeyValueEntryStream -> txFpRows.consume((txId, fp, row) -> {
                HeapFiler filer = new HeapFiler(row);
                long timestamp = UIO.readLong(filer, "timestamp");
                boolean tombstone = UIO.readBoolean(filer, "tombstone");
                long version = UIO.readLong(filer, "version");
                byte[] value = UIO.readByteArray(filer, "value");
                byte[] key = UIO.readByteArray(filer, "key");
                return txFpRawKeyValueEntryStream.stream(txId, fp, key, value, timestamp, tombstone, version, row);
            }),
            txFpKeyValueStream);
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

    @Override
    public boolean tombstonedFromRow(byte[] row) throws Exception {
        return row[8] == 1;
    }

    @Override
    public long versionFromRow(byte[] row) throws Exception {
        return UIO.bytesLong(row, 8 + 1);
    }
}
