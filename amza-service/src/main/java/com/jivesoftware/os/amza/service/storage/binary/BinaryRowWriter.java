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
import com.jivesoftware.os.amza.shared.filer.IWriteable;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.shared.wal.WALWriter;
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
    public int write(long txId, RowType rowType, RawRows rows, IndexableKeys indexableKeys, TxKeyPointerFpStream stream) throws Exception {
        HeapFiler memoryFiler = new HeapFiler();
        TLongArrayList offsets = new TLongArrayList();
        rows.consume(row -> {
            offsets.add(memoryFiler.getFilePointer());
            int length = (1 + 8) + row.length;
            UIO.writeInt(memoryFiler, length, "length");
            UIO.writeByte(memoryFiler, rowType.toByte(), "rowType");
            UIO.writeLong(memoryFiler, txId, "txId");
            memoryFiler.write(row);
            UIO.writeInt(memoryFiler, length, "length");
            return true;
        });

        byte[] bytes = memoryFiler.getBytes();
        long startFp;
        ioStats.wrote.addAndGet(bytes.length);
        synchronized (filer.lock()) {
            startFp = filer.length();
            System.out.println("PRE-WRITE: filer:" + filer + " length:" + filer.length() + " wrote:" + bytes.length);
            filer.seek(startFp); // seek to end of file.
            filer.write(bytes);
            filer.flush(false); // TODO expose to config
            System.out.println("POST-WRITE: filer:" + filer + " length:" + filer.length());
        }

        TLongIterator iter = offsets.iterator();
        indexableKeys.consume((key, valueTimestamp, valueTombstones) -> stream.stream(txId, key, valueTimestamp, valueTombstones, startFp + iter.next()));
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
            stream -> stream.stream(row),
            stream -> stream.stream(null, -1, false),
            (txId, key, valueTimestamp, valueTombstoned, fp) -> {
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
        System.out.println("LENGTH: filer:" + filer + " length:" + filer.length());
        return filer.length();
    }

    public void flush(boolean fsync) throws IOException {
        filer.flush(fsync);
    }

    public void close() throws IOException {
        filer.close();
    }

}
