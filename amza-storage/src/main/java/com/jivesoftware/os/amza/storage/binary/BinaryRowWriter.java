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

import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.wal.WALWriter;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.IWriteable;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class BinaryRowWriter implements WALWriter {

    private final IWriteable filer;
    private final IoStats ioStats;

    public BinaryRowWriter(IWriteable filer, IoStats ioStats) {
        this.filer = filer;
        this.ioStats = ioStats;
    }

    @Override
    public long[] write(List<Long> rowTxIds, List<RowType> rowType, List<byte[]> rows) throws Exception {
        long[] offsets = new long[rows.size()];
        HeapFiler memoryFiler = new HeapFiler();
        int i = 0;
        for (byte[] row : rows) {
            offsets[i] = memoryFiler.getFilePointer();
            int length = (1 + 8) + row.length;
            UIO.writeInt(memoryFiler, length, "length");
            UIO.writeByte(memoryFiler, rowType.get(i).toByte(), "rowType");
            UIO.writeLong(memoryFiler, rowTxIds.get(i), "txId");
            memoryFiler.write(row);
            UIO.writeInt(memoryFiler, length, "length");
            i++;
        }
        byte[] bytes = memoryFiler.getBytes();
        long startFp;
        ioStats.wrote.addAndGet(bytes.length);
        synchronized (filer.lock()) {
            startFp = filer.length();
            filer.seek(startFp); // seek to end of file.
            filer.write(bytes);
            filer.flush(false); // TODO expose to config
        }
        long[] rowPointers = new long[offsets.length];
        for (int j = 0; j < offsets.length; j++) {
            rowPointers[j] = startFp + offsets[j];
        }
        return rowPointers;
    }

    @Override
    public long[] writePrimary(List<Long> txId, List<byte[]> rows) throws Exception {
        long[] rowPointers = write(txId,
            Collections.nCopies(txId.size(), RowType.primary),
            rows);
        return rowPointers;
    }

    @Override
    public long writeHighwater(byte[] row) throws Exception {
        long[] rowPointers = write(Collections.singletonList(-1L),
            Collections.singletonList(RowType.highwater),
            Collections.singletonList(row));
        return rowPointers[0];
    }

    @Override
    public long writeSystem(byte[] row) throws Exception {
        long[] rowPointers = write(Collections.singletonList(-1L),
            Collections.singletonList(RowType.system),
            Collections.singletonList(row));
        return rowPointers[0];
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
