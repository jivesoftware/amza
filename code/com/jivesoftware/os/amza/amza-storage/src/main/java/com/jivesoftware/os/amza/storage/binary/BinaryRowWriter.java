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

import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.amza.shared.filer.IFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.storage.filer.MemoryFiler;
import java.util.ArrayList;
import java.util.List;

public class BinaryRowWriter implements WALWriter {

    private final IFiler filer;
    private final IoStats ioStats;

    public BinaryRowWriter(IFiler filer, IoStats ioStats) {
        this.filer = filer;
        this.ioStats = ioStats;
    }

    @Override
    public List<byte[]> write(List<Long> rowTxIds, List<Byte> rowType, List<byte[]> rows) throws Exception {
        List<Long> offests = new ArrayList<>();
        MemoryFiler memoryFiler = new MemoryFiler();
        int i = 0;
        for (byte[] row : rows) {
            offests.add(memoryFiler.getFilePointer());
            int length = (1 + 8) + row.length;
            UIO.writeInt(memoryFiler, length, "length");
            memoryFiler.write(rowType.get(i));
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
            filer.flush();
        }
        List<byte[]> rowPointers = new ArrayList<>();
        for (Long offest : offests) {
            rowPointers.add(UIO.longBytes(startFp + offest));
        }
        return rowPointers;
    }

    @Override
    public long getEndOfLastRow() throws Exception {
        synchronized (filer.lock()) {
            return filer.length();
        }
    }

}
