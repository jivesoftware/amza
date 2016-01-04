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

import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.amza.service.stats.IoStats;
import com.jivesoftware.os.amza.api.wal.WALWriter;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import java.io.IOException;

public class BinaryRowWriter implements WALWriter {

    private final IWriteable filer;
    private final IoStats ioStats;

    public BinaryRowWriter(IWriteable filer, IoStats ioStats) {
        this.filer = filer;
        this.ioStats = ioStats;
    }

    @Override
    public int write(long txId,
        RowType rowType,
        int estimatedNumberOfRows,
        int estimatedSizeInBytes,
        RawRows rows,
        IndexableKeys indexableKeys,
        TxKeyPointerFpStream stream) throws Exception {
        byte[] lengthBuffer = new byte[4];
        HeapFiler memoryFiler = new HeapFiler((estimatedNumberOfRows * (4 + 1 + 8 + 4)) + estimatedSizeInBytes);
        TLongArrayList offsets = new TLongArrayList();
        rows.consume(row -> {
            offsets.add(memoryFiler.getFilePointer());
            int length = (1 + 8) + row.length;
            UIO.writeInt(memoryFiler, length, "length", lengthBuffer);
            UIO.writeByte(memoryFiler, rowType.toByte(), "rowType");
            UIO.writeLong(memoryFiler, txId, "txId");
            memoryFiler.write(row, 0, row.length);
            UIO.writeInt(memoryFiler, length, "length", lengthBuffer);
            return true;
        });

        long l = memoryFiler.length();
        long startFp;
        ioStats.wrote.addAndGet(l);
        synchronized (filer.lock()) {
            startFp = filer.length();
            filer.seek(startFp); // seek to end of file.
            filer.write(memoryFiler.leakBytes(), 0, (int) l);
            filer.flush(false); // TODO expose to config
        }

        TLongIterator iter = offsets.iterator();
        indexableKeys.consume((prefix, key, valueTimestamp, valueTombstones, valueVersion)
            -> stream.stream(txId, prefix, key, valueTimestamp, valueTombstones, valueVersion, startFp + iter.next()));
        return offsets.size();
    }

    @Override
    public long writeHighwater(byte[] row) throws Exception {
        return writeRowInternal(row, RowType.highwater);
    }

    @Override
    public long writeSystem(byte[] row) throws Exception {
        return writeRowInternal(row, RowType.system);
    }

    private long writeRowInternal(byte[] row, RowType type) throws Exception {
        long[] fps = new long[1];
        write(-1L,
            type,
            1,
            row.length,
            stream -> stream.stream(row),
            stream -> stream.stream(null, null, -1, false, -1),
            (txId, prefix, key, valueTimestamp, valueTombstoned, valueVerion, fp) -> {
                fps[0] = fp;
                return true;
            });
        return fps[0];
    }

    @Override
    public long getEndOfLastRow() throws Exception {
        synchronized (filer.lock()) {
            return filer.length();
        }
    }

    public long length() throws IOException {
        return filer.length();
    }

    public void flush(boolean fsync) throws IOException {
        filer.flush(fsync);
    }

    public void close() throws IOException {
        filer.close();
    }

}
