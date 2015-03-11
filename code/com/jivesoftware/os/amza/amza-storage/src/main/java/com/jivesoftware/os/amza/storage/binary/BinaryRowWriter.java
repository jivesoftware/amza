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

import com.jivesoftware.os.amza.shared.RowWriter;
import com.jivesoftware.os.amza.storage.filer.IFiler;
import com.jivesoftware.os.amza.storage.filer.MemoryFiler;
import com.jivesoftware.os.amza.storage.filer.UIO;
import java.util.ArrayList;
import java.util.List;

public class BinaryRowWriter implements RowWriter<byte[]> {

    private final IFiler filer;

    public BinaryRowWriter(IFiler filer) {
        this.filer = filer;
    }

    @Override
    public List<byte[]> write(List<byte[]> rows, boolean append) throws Exception {
        List<Long> offests = new ArrayList<>();
        MemoryFiler memoryFiler = new MemoryFiler();
        for (byte[] row : rows) {
            offests.add(memoryFiler.getFilePointer());
            int length = row.length;
            UIO.writeInt(memoryFiler, length, "length");
            memoryFiler.write(row);
            UIO.writeInt(memoryFiler, length, "length");
        }

        long startFp;
        synchronized (filer.lock()) {
            if (append) {
                startFp = filer.length();
                filer.seek(startFp); // seek to end of file.
                filer.write(memoryFiler.getBytes());
            } else {
                startFp = 0;
                filer.seek(0);
                filer.write(memoryFiler.getBytes());
                filer.eof(); // trim file to size.
            }
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
