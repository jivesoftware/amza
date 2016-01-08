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
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.stream.Fps;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALReader;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.amza.service.stats.IoStats;
import com.jivesoftware.os.amza.service.storage.filer.WALFiler;
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
        byte[] intBuffer = new byte[4];
        synchronized (parent.lock()) {
            IReadable filer = parent.reader(null, parent.length(), 1024*1024);
            boolean valid = true;
            long seekTo = filer.length();
            for (int i = 0; i < corruptionParanoiaFactor; i++) {
                if (seekTo > 0) {
                    filer.seek(seekTo - 4);
                    int tailLength = UIO.readInt(filer, "length", intBuffer);
                    seekTo -= (tailLength + 8);
                    if (seekTo < 0) {
                        valid = false;
                        break;
                    } else {
                        filer.seek(seekTo);
                        int headLength = UIO.readInt(filer, "length", intBuffer);
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
        byte[] intLongBuffer = new byte[8];
        IReadable parentFiler = parent.reader(null, boundaryFp, 0);
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
                int readLength = parentFiler.read(page);

                HeapFiler filer = HeapFiler.fromBytes(page, readLength);
                long seekTo = filer.length() - 4;
                if (seekTo >= 0) {
                    while (true) {
                        long rowFP;
                        RowType rowType;
                        long rowTxId;
                        byte[] row;
                        filer.seek(seekTo);
                        int priorLength = UIO.readInt(filer, "priorLength", intLongBuffer);
                        if (seekTo < priorLength + 4) {
                            seekTo += 4;
                            pageSize = Math.max(4096, priorLength + 8); //TODO something smart
                            break;
                        }

                        seekTo -= (priorLength + 4);
                        filer.seek(seekTo);

                        int length = UIO.readInt(filer, "length", intLongBuffer);
                        filer.read(rowTypeByte);
                        rowType = RowType.fromByte(rowTypeByte[0]);
                        rowTxId = UIO.readLong(filer, "txId", intLongBuffer);
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
            byte[] intLongBuffer = new byte[8];
            while (fileLength < parent.length()) {
                fileLength = parent.length();
                filer = parent.reader(filer, fileLength, 1024*1024); //TODO config
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
                            length = UIO.readInt(filer, "length", intLongBuffer);
                        } catch (IOException x) {
                            if (!allowRepairs) {
                                throw x;
                            }
                        }
                        int lengthOfTypeAndTxId = 1 + 8;
                        if (length < lengthOfTypeAndTxId || offsetFp + length + 8 > fileLength) {
                            if (allowRepairs) {
                                return truncate(offsetFp);
                            } else {
                                String msg = "Scan terminated prematurely due to a corruption at fp:" + offsetFp + ". " + parent;
                                LOG.error(msg);
                                throw new EOFException(msg);
                            }
                        }
                        int trailingLength = -1;
                        try {
                            filer.read(rowTypeByte);
                            rowType = RowType.fromByte(rowTypeByte[0]);
                            rowTxId = UIO.readLong(filer, "txId", intLongBuffer);
                            row = new byte[length - lengthOfTypeAndTxId];
                            if (row.length > 0) {
                                filer.read(row);
                            }
                            trailingLength = UIO.readInt(filer, "length", intLongBuffer);
                        } catch (IOException x) {
                            if (!allowRepairs) {
                                throw x;
                            }
                        }
                        if (trailingLength < 0 || trailingLength != length) {
                            if (allowRepairs) {
                                return truncate(offsetFp);
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

    private boolean truncate(long offsetFp) throws IOException {
        // Corruption encoutered.
        // There is a huge assumption here that this is only called once at startup.
        // If this is encountred some time other than startup there will be data loss and WALIndex corruption.
        synchronized (parent.lock()) {
            long before = parent.length();
            parent.truncate(offsetFp);
            LOG.warn("Truncated corrupt WAL at {}, before={} after={} {}", offsetFp, before, parent.length(), parent);
            return false;
        }
    }

    @Override
    public byte[] readTypeByteTxIdAndRow(long position) throws IOException {
        int length = -1;
        try {
            IReadable filer = parent.reader(null, position + 4, 0);

            filer.seek(position);
            length = UIO.readInt(filer, "length", new byte[4]);

            filer = parent.reader(filer, position + 4 + length, 0);

            filer.seek(position + 4);
            byte[] row = new byte[length];
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
        IReadable[] filerRef = { parent.reader(null, 0, 0) };
        byte[] rawLength = new byte[4];
        byte[] rowTypeByteAndTxId = new byte[1 + 8];
        return fps.consume(fp -> {
            int length = -1;
            try {
                IReadable filer = parent.reader(filerRef[0], fp + 4, 0);
                filer.seek(fp);
                filer.read(rawLength);
                length = UIO.bytesInt(rawLength);

                filer = parent.reader(filer, fp + 4 + length, 0);
                filer.seek(fp + 4);
                filer.read(rowTypeByteAndTxId);
                RowType rowType = RowType.fromByte(rowTypeByteAndTxId[0]);
                long rowTxId = UIO.bytesLong(rowTypeByteAndTxId, 1);

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

    public void hackTruncation(int numBytes) {
        try {
            truncate(parent.length() - numBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
