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
import com.jivesoftware.os.amza.storage.filer.UIO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BinaryRowWriter implements RowWriter<byte[]> {

    private final IFiler filer;

    public BinaryRowWriter(IFiler filer) {
        this.filer = filer;
    }

    @Override
    public List<byte[]> write(List<byte[]> rows, boolean append) throws Exception {
        List<byte[]> rowPointers;
        synchronized (filer.lock()) {
            if (append) {
                filer.seek(filer.length()); // seek to end of file.
                rowPointers = writeRows(rows);
            } else {
                filer.seek(0);
                rowPointers = writeRows(rows);
                filer.eof(); // trim file to size.
            }
            filer.flush();
        }
        return rowPointers;
    }

    private List<byte[]> writeRows(List<byte[]> rows) throws IOException {
        List<byte[]> rowPointers = new ArrayList<>();
        for (byte[] row : rows) {
            rowPointers.add(UIO.longBytes(filer.getFilePointer()));
            int length = row.length;
            UIO.writeInt(filer, length, "length");
            filer.write(row);
            UIO.writeInt(filer, length, "length");
        }
        return rowPointers;
    }
}
