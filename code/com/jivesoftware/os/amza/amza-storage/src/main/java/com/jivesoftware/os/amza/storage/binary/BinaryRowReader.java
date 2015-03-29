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

import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.storage.filer.Filer;
import com.jivesoftware.os.amza.storage.filer.FilerChannel;
import java.io.IOException;

public class BinaryRowReader implements WALReader {

    private final Filer parent; // TODO use mem-mapping and bb.dupliate to remove all the hard locks
    private final IoStats ioStats;

    public BinaryRowReader(Filer parent, IoStats ioStats) {
        this.parent = parent;
        this.ioStats = ioStats;
    }

    @Override
    public void reverseScan(RowStream stream) throws Exception {
        long seekTo;
        synchronized (parent.lock()) {
            seekTo = parent.length() - 4; // last length int
        }
        FilerChannel filer = parent.fileChannelFiler();
        if (seekTo < 0) {
            return;
        }
        long read = 0;
        try {
            while (true) {
                long rowFP;
                byte rowType;
                long rowTxId;
                byte[] row;
                if (seekTo >= 0) {
                    filer.seek(seekTo);
                    int priorLength = UIO.readInt(filer, "priorLength");
                    seekTo -= (priorLength + 4);
                    if (seekTo < 0) {
                        return;
                    }
                    filer.seek(seekTo);

                    int length = UIO.readInt(filer, "length");
                    rowType = (byte) filer.read();
                    rowTxId = UIO.readLong(filer, "txId");
                    row = new byte[length - (1 + 8)];
                    filer.read(row);
                    rowFP = seekTo;
                    read += (filer.getFilePointer() - seekTo);
                    seekTo -= 4;
                } else {
                    break;
                }

                if (!stream.row(rowFP, rowTxId, rowType, row)) {
                    return;
                }
            }
        } finally {
            ioStats.read.addAndGet(read);
        }
    }

    @Override
    public void scan(long offset, RowStream stream) throws Exception {
        long fileLength = 0;
        long read = 0;
        try {
            while (fileLength < parent.length()) {
                synchronized (parent.lock()) {
                    fileLength = parent.length();
                }
                FilerChannel filer = parent.fileChannelFiler();
                while (true) {
                    long rowFP;
                    long rowTxId;
                    byte rowType;
                    byte[] row;
                    filer.seek(offset);
                    if (offset < fileLength) {
                        rowFP = offset;
                        int length = UIO.readInt(filer, "length");
                        rowType = (byte) filer.read();
                        rowTxId = UIO.readLong(filer, "txId");
                        row = new byte[length - (1 + 8)];
                        if (length > 1) {
                            filer.read(row);
                        }
                        UIO.readInt(filer, "length");
                        long fp = filer.getFilePointer();
                        read += (fp - offset);
                        offset = fp;
                    } else {
                        break;
                    }
                    if (!stream.row(rowFP, rowTxId, rowType, row)) {
                        return;
                    }
                }
            }
        } finally {
            ioStats.read.addAndGet(read);
        }
    }

    @Override
    public byte[] read(byte[] rowPointer) throws IOException {
        long fileLength;
        synchronized (parent.lock()) {
            fileLength = parent.length();
        }
        if (fileLength == 0) {
            return null;
        }
        FilerChannel filer = parent.fileChannelFiler();
        filer.seek(UIO.bytesLong(rowPointer));
        int length = UIO.readInt(filer, "length");
        long rowType = (byte) filer.read();
        byte[] row = new byte[length - 1];
        if (length > 1) {
            filer.read(row);
        }
        return row;
    }
}
