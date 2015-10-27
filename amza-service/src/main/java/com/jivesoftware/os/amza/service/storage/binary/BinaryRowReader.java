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

import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.storage.filer.WALFiler;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.shared.stream.Fps;
import com.jivesoftware.os.amza.shared.wal.WALReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.EOFException;
import java.io.IOException;

public class BinaryRowReader implements WALReader {

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
            IReadable filer = parent.fileChannelFiler();
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
    public boolean reverseScan(RowStream stream) throws Exception {
        long boundaryFp = parent.length();
        if (boundaryFp == 0) {
            return true;
        }
        IReadable parentFiler = parent.bestFiler(null, boundaryFp);
        long read = 0;
        try {
            int pageSize = 1024 * 1024;
            byte[] page = new byte[pageSize];
            byte[] rowTypeByte = new byte[1];

            while (true) {
                long nextBoundaryFp = Math.max(boundaryFp - pageSize, 0);
                int nextPageSize = (int) (boundaryFp - nextBoundaryFp);
                if (page.length != nextPageSize) {
                    page = new byte[nextPageSize];
                }

                parentFiler.seek(nextBoundaryFp);
                parentFiler.read(page);

                HeapFiler filer = new HeapFiler(page);
                long seekTo = filer.length() - 4;
                if (seekTo >= 0) {
                    while (true) {
                        long rowFP;
                        RowType rowType;
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
                        filer.read(rowTypeByte);
                        rowType = RowType.fromByte(rowTypeByte[0]);
                        rowTxId = UIO.readLong(filer, "txId");
                        row = new byte[length - (1 + 8)];
                        if (row.length > 0) {
                            filer.read(row);
                        }
                        rowFP = nextBoundaryFp + seekTo;
                        read += (filer.getFilePointer() - seekTo);
                        if (rowType != null) {
                            if (!stream.row(rowFP, rowTxId, rowType, row)) {
                                return false;
                            }
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
            return true;
        } finally {
            ioStats.read.addAndGet(read);
        }
    }

    @Override
    public boolean scan(long offsetFp, boolean allowRepairs, RowStream stream) throws Exception {
        long fileLength = 0;
        long read = 0;
        try {
            IReadable filer = null;
            byte[] rowTypeByte = new byte[1];
            while (fileLength < parent.length()) {
                fileLength = parent.length();
                filer = parent.bestFiler(filer, fileLength);
                while (true) {
                    long rowFP;
                    long rowTxId = -1;
                    RowType rowType = null;
                    byte[] row = null;
                    filer.seek(offsetFp);
                    if (offsetFp < fileLength) {
                        rowFP = offsetFp;
                        int length = -1;
                        try {
                            length = UIO.readInt(filer, "length");
                        } catch (IOException x) {
                            if (!allowRepairs) {
                                throw x;
                            }
                        }
                        int lengthOfTypeAndTxId = 1 + 8;
                        if (length < lengthOfTypeAndTxId || offsetFp + length + 8 > fileLength) {
                            if (allowRepairs) {
                                return truncate(filer, offsetFp);
                            } else {
                                String msg = "Scan terminated prematurely due a corruption at fp:" + offsetFp + ". " + parent;
                                LOG.error(msg);
                                throw new EOFException(msg);
                            }
                        }
                        int trailingLength = -1;
                        try {
                            filer.read(rowTypeByte);
                            rowType = RowType.fromByte(rowTypeByte[0]);
                            rowTxId = UIO.readLong(filer, "txId");
                            row = new byte[length - lengthOfTypeAndTxId];
                            if (row.length > 0) {
                                filer.read(row);
                            }
                            trailingLength = UIO.readInt(filer, "length");
                        } catch (IOException x) {
                            if (!allowRepairs) {
                                throw x;
                            }
                        }
                        if (trailingLength < 0 || trailingLength != length) {
                            if (allowRepairs) {
                                return truncate(filer, offsetFp);
                            } else {
                                throw new IOException("The lead length of " + length + " didn't equal trailing length of " + trailingLength);
                            }
                        }
                        long fp = filer.getFilePointer();
                        read += (fp - offsetFp);
                        offsetFp = fp;
                    } else {
                        break;
                    }
                    if (rowType != null) {
                        if (!stream.row(rowFP, rowTxId, rowType, row)) {
                            return false;
                        }
                    }
                }
            }
            return true;
        } finally {
            ioStats.read.addAndGet(read);
        }
    }

    private boolean truncate(IReadable filer, long offsetFp) throws IOException {
        // Corruption encoutered.
        // There is a huge assumption here that this is only called once at startup.
        // If this is encountred some time other than startup there will be data loss and WALIndex corruption.
        filer.seek(offsetFp);
        synchronized (parent.lock()) {
            LOG.warn("Truncated corrupt WAL at " + offsetFp + " in " + parent);
            parent.seek(offsetFp);
            parent.eof();
            return false;
        }
    }

    @Override
    public byte[] read(long position) throws IOException {
        int length = -1;
        try {
            IReadable filer = parent.bestFiler(null, position + 4);

            filer.seek(position);
            length = UIO.readInt(filer, "length");

            filer = parent.bestFiler(filer, position + 4 + length);

            filer.seek(position + 4 + 1 + 8);
            byte[] row = new byte[length - (1 + 8)];
            if (row.length > 0) {
                filer.read(row);
            }
            return row;
        } catch (NegativeArraySizeException x) {
            LOG.error("FAILED to read length:" + length + " bytes at position:" + position + " in file:" + parent);
            throw x;
        }
    }

    @Override
    public boolean read(Fps fps, RowStream rowStream) throws Exception {
        IReadable[] filerRef = { parent.bestFiler(null, 0) };
        byte[] rowTypeByte = new byte[1];
        return fps.consume(fp -> {
            int length = -1;
            try {
                IReadable filer = parent.bestFiler(filerRef[0], fp + 4);
                filer.seek(fp);
                length = UIO.readInt(filer, "length");

                filer = parent.bestFiler(filer, fp + 4 + length);
                filer.seek(fp + 4);
                filer.read(rowTypeByte);
                RowType rowType = RowType.fromByte(rowTypeByte[0]);
                long rowTxId = UIO.readLong(filer, "txId");

                byte[] row = new byte[length - (1 + 8)];
                if (row.length > 0) {
                    filer.read(row);
                }

                filerRef[0] = filer;
                return rowStream.row(fp, rowTxId, rowType, row);
            } catch (NegativeArraySizeException x) {
                LOG.error("FAILED to read length:" + length + " bytes at position:" + fp + " in file:" + parent);
                throw x;
            }
        });
    }
}
