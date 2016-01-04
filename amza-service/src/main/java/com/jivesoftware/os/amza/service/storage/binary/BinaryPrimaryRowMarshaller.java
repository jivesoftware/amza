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
import com.jivesoftware.os.amza.api.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import java.io.IOException;
import org.xerial.snappy.Snappy;

public class BinaryPrimaryRowMarshaller implements PrimaryRowMarshaller {

    @Override
    public byte[] toRow(RowType rowType, byte[] pk, byte[] value, long timestamp, boolean tombstoned, long version) throws Exception {

        return toRowBytes(compress(rowType, pk), compress(rowType, value), timestamp, tombstoned, version);
    }

    private byte[] toRowBytes(byte[] pk, byte[] value, long timestamp, boolean tombstoned, long version) throws IOException {
        byte[] lengthBuffer = new byte[4];
        HeapFiler filer = new HeapFiler(8 + 1 + 8 + 4 + (value != null ? value.length : 0) + 4 + pk.length);
        UIO.writeLong(filer, timestamp, "timestamp");
        UIO.writeByte(filer, tombstoned ? (byte) 1 : (byte) 0, "tombstone");
        UIO.writeLong(filer, version, "version");

        UIO.writeByteArray(filer, value, "value", lengthBuffer);
        UIO.writeByteArray(filer, pk, "key", lengthBuffer);
        return filer.getBytes();
    }

    @Override
    public byte[] convert(RowType fromType, byte[] row, RowType toType) throws Exception {
        if (fromType == toType) {
            return row;
        }
        byte[] intLongBuffer = new byte[8];
        HeapFiler filer = HeapFiler.fromBytes(row, row.length);
        long timestamp = UIO.readLong(filer, "timestamp", intLongBuffer);
        boolean tombstone = UIO.readBoolean(filer, "tombstone");
        long version = UIO.readLong(filer, "version", intLongBuffer);
        byte[] value = UIO.readByteArray(filer, "value", intLongBuffer);
        byte[] pk = UIO.readByteArray(filer, "key", intLongBuffer);
        return toRow(toType, uncompress(fromType, pk), uncompress(fromType, value), timestamp, tombstone, version);
    }

    @Override
    public int maximumSizeInBytes(RowType rowType, int pkSizeInBytes, int valueSizeInBytes) {
        if (rowType == RowType.snappy_primary) {
            return 8 + 1 + 8 + 4 + Snappy.maxCompressedLength(valueSizeInBytes) + 4 + Snappy.maxCompressedLength(pkSizeInBytes);
        } else {
            return 8 + 1 + 8 + 4 + valueSizeInBytes + 4 + pkSizeInBytes;
        }
    }

    @Override
    public boolean fromRows(FpRows fpRows, FpKeyValueStream fpKeyValueStream) throws Exception {
        byte[] intLongBuffer = new byte[8];
        return WALKey.decompose(
            stream -> fpRows.consume((fp, rowType, row) -> {
                HeapFiler filer = HeapFiler.fromBytes(row, row.length);
                long timestamp = UIO.readLong(filer, "timestamp", intLongBuffer);
                boolean tombstone = UIO.readBoolean(filer, "tombstone");
                long version = UIO.readLong(filer, "version", intLongBuffer);
                byte[] value = UIO.readByteArray(filer, "value", intLongBuffer);
                byte[] pk = UIO.readByteArray(filer, "key", intLongBuffer);
                return stream.stream(-1, fp, rowType, uncompress(rowType, pk), uncompress(rowType, value), timestamp, tombstone, version, row);
            }),
            (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row)
                -> fpKeyValueStream.stream(fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion));
    }

    @Override
    public boolean fromRows(TxFpRows txFpRows, TxKeyValueStream txKeyValueStream) throws Exception {
        byte[] intLongBuffer = new byte[8];
        return WALKey.decompose(
            stream -> txFpRows.consume((txId, fp, rowType, row) -> {
                HeapFiler filer = HeapFiler.fromBytes(row, row.length);
                long timestamp = UIO.readLong(filer, "timestamp", intLongBuffer);
                boolean tombstone = UIO.readBoolean(filer, "tombstone");
                long version = UIO.readLong(filer, "version", intLongBuffer);
                byte[] value = UIO.readByteArray(filer, "value", intLongBuffer);
                byte[] pk = UIO.readByteArray(filer, "key", intLongBuffer);
                return stream.stream(txId, fp, rowType, uncompress(rowType, pk), uncompress(rowType, value), timestamp, tombstone, version, row);
            }),
            (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row)
                -> txKeyValueStream.stream(txId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion));
    }

    @Override
    public boolean fromRows(TxFpRows txFpRows, UnprefixedTxKeyValueStream txKeyValueStream) throws Exception {
        byte[] intLongBuffer = new byte[8];
        return WALKey.decompose(
            stream -> txFpRows.consume((txId, fp, rowType, row) -> {
                HeapFiler filer = HeapFiler.fromBytes(row, row.length);
                long timestamp = UIO.readLong(filer, "timestamp", intLongBuffer);
                boolean tombstone = UIO.readBoolean(filer, "tombstone");
                long version = UIO.readLong(filer, "version", intLongBuffer);
                byte[] value = UIO.readByteArray(filer, "value", intLongBuffer);
                byte[] pk = UIO.readByteArray(filer, "key", intLongBuffer);
                return stream.stream(txId, fp, rowType, uncompress(rowType, pk), uncompress(rowType, value), timestamp, tombstone, version, row);
            }),
            (txId, fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, row)
                -> txKeyValueStream.row(txId, key, value, valueTimestamp, valueTombstoned, valueVersion));
    }

    @Override
    public boolean fromRows(TxFpRows txFpRows, WALKey.TxFpKeyValueEntryStream<byte[]> txFpKeyValueStream) throws Exception {
        byte[] intLongBuffer = new byte[8];
        return WALKey.decompose(
            txFpRawKeyValueEntryStream -> txFpRows.consume((txId, fp, rowType, row) -> {
                HeapFiler filer = HeapFiler.fromBytes(row, row.length);
                long timestamp = UIO.readLong(filer, "timestamp", intLongBuffer);
                boolean tombstone = UIO.readBoolean(filer, "tombstone");
                long version = UIO.readLong(filer, "version", intLongBuffer);
                byte[] value = UIO.readByteArray(filer, "value", intLongBuffer);
                byte[] pk = UIO.readByteArray(filer, "key", intLongBuffer);
                return txFpRawKeyValueEntryStream.stream(txId, fp, rowType, uncompress(rowType, pk), uncompress(rowType, value), timestamp, tombstone, version,
                    row);
            }),
            txFpKeyValueStream);
    }

    @Override
    public byte[] valueFromRow(RowType rowType, byte[] row, int offset) throws Exception {
        HeapFiler filer = HeapFiler.fromBytes(row, row.length);
        filer.seek(offset + 8 + 1 + 8);
        return uncompress(rowType, UIO.readByteArray(filer, "value", new byte[4]));
    }

    @Override
    public long timestampFromRow(byte[] row, int offset) throws Exception {
        return UIO.bytesLong(row, offset);
    }

    @Override
    public boolean tombstonedFromRow(byte[] row, int offset) throws Exception {
        return row[offset + 8] == 1;
    }

    @Override
    public long versionFromRow(byte[] row, int offset) throws Exception {
        return UIO.bytesLong(row, offset + 8 + 1);
    }

    private byte[] compress(RowType rowType, byte[] bytes) throws IOException {
        if (rowType == RowType.primary) {
            return bytes;
        } else if (rowType == RowType.snappy_primary) {
            if (bytes != null) {
                return Snappy.compress(bytes);
            }
            return null;
        } else {
            throw new IllegalArgumentException("compressing rowType:" + rowType + " is not supported.");
        }
    }

    private byte[] uncompress(RowType rowType, byte[] bytes) throws IOException {
        if (rowType == RowType.primary) {
            return bytes;
        } else if (rowType == RowType.snappy_primary) {
            if (bytes != null) {
                return Snappy.uncompress(bytes);
            }
            return null;
        } else {
            throw new IllegalArgumentException("uncompressing rowType:" + rowType + " is not supported.");
        }
    }
}
