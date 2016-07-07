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

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALWriter;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.amza.service.stats.IoStats;
import com.jivesoftware.os.amza.service.storage.filer.WALFiler;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import java.io.IOException;

public class BinaryRowWriter implements WALWriter {

    private final IAppendOnly appendOnly;
    private final IoStats ioStats;

    public BinaryRowWriter(WALFiler filer, IoStats ioStats) throws IOException {
        this.appendOnly = filer.appender();
        this.ioStats = ioStats;
    }

    @Override
    public int write(long txId,
        RowType rowType,
        int estimatedNumberOfRows,
        int estimatedSizeInBytes,
        RawRows rows,
        IndexableKeys indexableKeys,
        TxKeyPointerFpStream stream,
        boolean addToLeapCount,
        boolean hardFsyncBeforeLeapBoundary) throws Exception {

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
        synchronized (appendOnly.lock()) {
            startFp = appendOnly.length();
            appendOnly.write(memoryFiler.leakBytes(), 0, (int) l);
            appendOnly.flush(false); // TODO expose to config
        }

        TLongIterator iter = offsets.iterator();
        indexableKeys.consume((prefix, key, value, valueTimestamp, valueTombstones, valueVersion)
            -> stream.stream(txId, prefix, key, value, valueTimestamp, valueTombstones, valueVersion, startFp + iter.next()));
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
            stream -> stream.stream(null, null, null, -1, false, -1),
            (txId, prefix, key, value, valueTimestamp, valueTombstoned, valueVerion, fp) -> {
                fps[0] = fp;
                return true;
            },
            true,
            false);
        return fps[0];
    }

    @Override
    public long getEndOfLastRow() throws Exception {
        synchronized (appendOnly.lock()) {
            return appendOnly.length();
        }
    }

    public long length() throws IOException {
        return appendOnly.length();
    }

    public void flush(boolean fsync) throws IOException {
        appendOnly.flush(fsync);
    }

    public void close() throws IOException {
        appendOnly.close();
    }

}
