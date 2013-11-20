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

import com.jivesoftware.os.amza.shared.TableRowWriter;
import com.jivesoftware.os.amza.storage.chunks.IFiler;
import com.jivesoftware.os.amza.storage.chunks.UIO;
import java.io.IOException;
import java.util.Collection;

public class BinaryRowWriter implements TableRowWriter<byte[]> {

    private final IFiler filer;

    public BinaryRowWriter(IFiler filer) {
        this.filer = filer;
    }

    @Override
    public void write(Collection<byte[]> rows, boolean append) throws Exception {
        synchronized (filer.lock()) {
            if (append) {
                filer.seek(filer.length()); // seek to end of file.
                writeRows(rows);
            } else {
                filer.seek(0);
                writeRows(rows);
                filer.eof(); // trim file to size.
            }
            filer.flush();
        }
    }

    private void writeRows(Collection<byte[]> rows) throws IOException {
        for (byte[] row : rows) {
            UIO.writeInt(filer, row.length, "length");
            filer.write(row);
            UIO.writeInt(filer, row.length, "length");
        }
    }
}
