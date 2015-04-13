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
import com.jivesoftware.os.amza.shared.filer.IReadable;
import com.jivesoftware.os.amza.shared.filer.MemoryFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.storage.filer.WALFiler;
import com.jivesoftware.os.amza.storage.filer.WALFilerChannelReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

public class BinaryRowReader implements WALReader {

    public static void main(String[] args) throws Exception {
        String rowFile = "/jive/peek/332267939997237250.kvt";
        WALFiler walFiler = new WALFiler(rowFile, "rw");
        BinaryRowReader reader = new BinaryRowReader(walFiler, new IoStats(), 10);
        System.out.println("rowFP\trowTxId\trowType\trow.length\trowBytes");

        reader.scan(0, false, new RowStream() {

            @Override
            public boolean row(long rowFP, long rowTxId, byte rowType, byte[] row) throws Exception {
                System.out.println(rowFP + "\t" + rowTxId + "\t" + rowType + "\t" + row.length + "\t" + Arrays.toString(row));
                return true;
            }
        });
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final WALFiler parent; // TODO use mem-mapping and bb.dupliate to remove all the hard locks
    private final IoStats ioStats;
    private final int corruptionParanoiaFactor;

    public BinaryRowReader(WALFiler parent, IoStats ioStats, int corruptionParanoiaFactor) {
        this.parent = parent;
        this.ioStats = ioStats;
        this.corruptionParanoiaFactor = corruptionParanoiaFactor;
    }

    boolean validate() throws IOException {
        synchronized (parent.lock()) {
            WALFilerChannelReader filer = parent.fileChannelFiler();
            boolean valid = true;
            long seekTo = filer.length();
            for (int i = 0; i < corruptionParanoiaFactor; i++) {
                if (seekTo > 0) {
                    filer.seek(seekTo - 4);
                    int tailLength = UIO.readInt(filer, "length");
                    seekTo -= (tailLength + 8);
                    if (seekTo < 0) {
                        valid = false;
                        break;
                    } else {
                        filer.seek(seekTo);
                        int headLength = UIO.readInt(filer, "length");
                        if (tailLength != headLength) {
                            LOG.error("Read a head length of " + headLength + " and tail length of " + tailLength);
                            valid = false;
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
            return valid;
        }
    }

    @Override
    public void reverseScan(RowStream stream) throws Exception {
        long boundaryFp;
        synchronized (parent.lock()) {
            boundaryFp = parent.length();
        }
        IReadable parentFiler = parent.fileChannelMemMapFiler(boundaryFp);
        if (parentFiler == null) {
            parentFiler = parent.fileChannelFiler();
        }
        if (boundaryFp == 0) {
            return;
        }
        long read = 0;
        try {
            int pageSize = 1024 * 1024;
            byte[] page = new byte[pageSize];

            while (true) {
                long nextBoundaryFp = Math.max(boundaryFp - pageSize, 0);
                int nextPageSize = (int) (boundaryFp - nextBoundaryFp);
                if (page.length != nextPageSize) {
                    page = new byte[nextPageSize];
                }

                parentFiler.seek(nextBoundaryFp);
                parentFiler.read(page);

                MemoryFiler filer = new MemoryFiler(page);
                long seekTo = filer.length() - 4;
                if (seekTo >= 0) {
                    while (true) {
                        long rowFP;
                        byte rowType;
                        long rowTxId;
                        byte[] row;
                        filer.seek(seekTo);
                        int priorLength = UIO.readInt(filer, "priorLength");
                        if (seekTo < priorLength + 4) {
                            seekTo += 4;
                            pageSize = Math.max(4096, priorLength + 8); //TODO something smart
                            break;
                        }

                        seekTo -= (priorLength + 4);
                        filer.seek(seekTo);

                        int length = UIO.readInt(filer, "length");
                        rowType = (byte) filer.read();
                        rowTxId = UIO.readLong(filer, "txId");
                        row = new byte[length - (1 + 8)];
                        if (row.length > 0) {
                            filer.read(row);
                        }
                        rowFP = nextBoundaryFp + seekTo;
                        read += (filer.getFilePointer() - seekTo);

                        if (!stream.row(rowFP, rowTxId, rowType, row)) {
                            return;
                        }

                        if (seekTo >= 4) {
                            seekTo -= 4;
                        } else {
                            break;
                        }
                    }
                    boundaryFp = nextBoundaryFp + seekTo;
                } else {
                    break;
                }
            }
        } finally {
            ioStats.read.addAndGet(read);
        }
    }

    @Override
    public void scan(long offset, boolean allowRepairs, RowStream stream) throws Exception {
        long fileLength = 0;
        long read = 0;
        try {
            while (fileLength < parent.length()) {
                synchronized (parent.lock()) {
                    fileLength = parent.length();
                }
                IReadable filer = parent.fileChannelMemMapFiler(fileLength);
                if (filer == null) {
                    filer = parent.fileChannelFiler();
                }
                while (true) {
                    long rowFP;
                    long rowTxId;
                    byte rowType;
                    byte[] row;
                    filer.seek(offset);
                    if (offset < fileLength) {
                        rowFP = offset;
                        int length = UIO.readInt(filer, "length");
                        if (offset + length + 8 > fileLength) {
                            if (allowRepairs) {
                                // Corruption encoutered.
                                // There is a huge assumption here that this is only called once at startup.
                                // If this is encountred some time other than startup there will be data loss and WALIndex corruption.
                                filer.seek(offset);
                                synchronized (parent.lock()) {
                                    LOG.warn("Truncated corrupt WAL. " + parent);
                                    parent.seek(offset);
                                    parent.eof();
                                    return;
                                }
                            } else {
                                String msg = "Scan terminated prematurely due a corruption at fp:" + offset + ". " + parent;
                                LOG.error(msg);
                                throw new EOFException(msg);
                            }
                        }
                        rowType = (byte) filer.read();
                        rowTxId = UIO.readLong(filer, "txId");
                        row = new byte[length - (1 + 8)];
                        if (row.length > 0) {
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
    public byte[] read(long position) throws IOException {
        long fileLength = -1;
        int length = -1;
        try {

            synchronized (parent.lock()) {
                fileLength = parent.length();
            }
            if (fileLength == 0) {
                return null;
            }

            IReadable filer = parent.fileChannelMemMapFiler(position + 4);
            if (filer == null) {
                filer = parent.fileChannelFiler();
            }

            filer.seek(position);
            length = UIO.readInt(filer, "length");

            if (position + 4 + length > filer.length()) {
                filer = parent.fileChannelMemMapFiler(position + 4 + length);
                if (filer == null) {
                    filer = parent.fileChannelFiler();
                }
            }

            filer.seek(position + 4 + 1 + 8);
            byte[] row = new byte[length - (1 + 8)];
            if (row.length > 0) {
                filer.read(row);
            }
            return row;
        } catch (NegativeArraySizeException x) {
            LOG.error("FAILED to read length:" + length + " bytes at position:" + position + " in file of length:" + fileLength + " " + parent);
            throw x;
        }
    }

}
