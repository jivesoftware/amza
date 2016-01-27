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
import com.jivesoftware.os.amza.api.wal.RowIO.PreTruncationNotifier;
import com.jivesoftware.os.amza.api.wal.RowIO.ValidationStream;
import com.jivesoftware.os.amza.api.wal.WALReader;
import com.jivesoftware.os.amza.service.stats.IoStats;
import com.jivesoftware.os.amza.service.storage.filer.WALFiler;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.EOFException;
import java.io.IOException;

public class BinaryRowReader implements WALReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final WALFiler parent;
    private final IoStats ioStats;

    public BinaryRowReader(WALFiler parent, IoStats ioStats) {
        this.parent = parent;
        this.ioStats = ioStats;
    }

    void validate(boolean backwardScan,
        boolean truncateToLastRowFp,
        ValidationStream backward,
        ValidationStream forward,
        PreTruncationNotifier preTruncationNotifier) throws Exception {

        byte[] intLongBuffer = new byte[8];
        synchronized (parent.lock()) {
            IReadable filer = parent.reader(null, parent.length(), 1024 * 1024);
            long filerLength = filer.length();
            if (backwardScan) {
                long seekTo = filerLength;
                while (seekTo > 0) {
                    if (seekTo < 4) {
                        LOG.error("Validation had insufficient bytes to read tail length at offset {} with file length {}", seekTo, filerLength);
                        break;
                    }

                    filer.seek(seekTo - 4);
                    int tailLength = UIO.readInt(filer, "length", intLongBuffer);
                    if (tailLength <= 0 || tailLength >= filerLength) {
                        LOG.error("Validation found tail length of {} at offset {} with file length {}", tailLength, seekTo, filerLength);
                        break;
                    }
                    seekTo = seekTo - tailLength - 8;
                    if (seekTo < 0) {
                        LOG.error("Validation required seek to {} with file length {}", seekTo, filerLength);
                        break;
                    } else {
                        filer.seek(seekTo);
                        int headLength = UIO.readInt(filer, "length", intLongBuffer);
                        if (tailLength != headLength) {
                            LOG.warn("Validation read a head length of {} but a tail length of {} at offset {} with file length {}",
                                headLength, tailLength, seekTo, filerLength);
                            break;
                        }

                        RowType rowType = RowType.fromByte((byte) filer.read());
                        long rowTxId = UIO.readLong(filer, "txId", intLongBuffer);
                        byte[] row = new byte[headLength - (1 + 8)];
                        filer.read(row);

                        long truncateAfterRowAtFp;
                        try {
                            truncateAfterRowAtFp = backward.row(seekTo, rowTxId, rowType, row);
                        } catch (IOException e) {
                            LOG.warn("Validation encountered an I/O exception at offset {} with file length {}", new Object[] { seekTo, filerLength }, e);
                            break;
                        }

                        if (truncateAfterRowAtFp > -1) {
                            if (truncateToLastRowFp) {
                                filer.seek(truncateAfterRowAtFp);
                                headLength = UIO.readInt(filer, "length", intLongBuffer);
                                long truncatedLength = truncateAfterRowAtFp + 4 + headLength + 4;
                                if (truncatedLength != filerLength) {
                                    LOG.warn("Truncating after row at fp {} for reverse scan", truncateAfterRowAtFp);
                                    truncate(preTruncationNotifier, truncatedLength);
                                }
                            }
                            return;
                        }
                    }
                    if (seekTo == 0) {
                        LOG.warn("Truncating entire WAL");
                        truncate(preTruncationNotifier, 0);
                        return;
                    }
                }
            }

            long[] truncateAfterRowAtFp = new long[] { Long.MIN_VALUE };
            scan(0, true, preTruncationNotifier, (rowFP, rowTxId, rowType, row) -> {
                long result = forward.row(rowFP, rowTxId, rowType, row);
                if (result != -1) {
                    if (result < -1) {
                        truncateAfterRowAtFp[0] = -(result + 1);
                    }
                    if (result > -1) {
                        return false;
                    }
                }
                return true;
            });
            if (truncateAfterRowAtFp[0] == Long.MIN_VALUE) {
                if (truncateToLastRowFp) {
                    LOG.warn("Truncating entire WAL due to missing truncation feedback");
                    truncate(preTruncationNotifier, 0);
                }
            } else {
                // Have to reacquire filer because scan may have truncated
                filer = parent.reader(filer, parent.length(), 1024 * 1024);
                filer.seek(truncateAfterRowAtFp[0]);
                long headLength = UIO.readInt(filer, "length", intLongBuffer);
                long truncatedLength = truncateAfterRowAtFp[0] + 4 + headLength + 4;
                if (truncatedLength < filer.length()) {
                    LOG.warn("Truncating after row at fp {} for forward scan", truncateAfterRowAtFp[0]);
                    truncate(preTruncationNotifier, truncatedLength);
                }
            }
        }
    }

    @Override
    public boolean reverseScan(RowStream stream) throws Exception {
        long boundaryFp = parent.length(); // last length int
        IReadable parentFiler = parent.reader(null, boundaryFp, 0);
        if (boundaryFp < 0) {
            return true;
        }
        long read = 0;
        try {
            byte[] intLongBuffer = new byte[8];
            long seekTo = boundaryFp - 4;
            while (true) {
                long rowFP;
                RowType rowType;
                long rowTxId;
                byte[] row;
                if (seekTo >= 0) {
                    parentFiler.seek(seekTo);
                    int priorLength = UIO.readInt(parentFiler, "priorLength", intLongBuffer);
                    seekTo -= (priorLength + 4);
                    if (seekTo < 0) {
                        break;
                    }
                    parentFiler.seek(seekTo);

                    int length = UIO.readInt(parentFiler, "length", intLongBuffer);
                    rowType = RowType.fromByte((byte) parentFiler.read());
                    rowTxId = UIO.readLong(parentFiler, "txId", intLongBuffer);
                    row = new byte[length - (1 + 8)];
                    parentFiler.read(row);
                    rowFP = seekTo;
                    read += (parentFiler.getFilePointer() - seekTo);
                    seekTo -= 4;
                } else {
                    break;
                }

                if (!stream.row(rowFP, rowTxId, rowType, row)) {
                    return false;
                }
            }
            return true;
        } finally {
            ioStats.read.addAndGet(read);
        }
    }

    /*@Override
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
    }*/
    @Override
    public boolean scan(long offsetFp, boolean allowRepairs, RowStream stream) throws Exception {
        return scan(offsetFp, allowRepairs, null, stream);
    }

    private boolean scan(long offsetFp, boolean allowRepairs, PreTruncationNotifier preTruncationNotifier, RowStream stream) throws Exception {
        long fileLength = 0;
        long read = 0;
        try {
            IReadable filer = null;
            byte[] rowTypeByte = new byte[1];
            byte[] intLongBuffer = new byte[8];
            while (fileLength < parent.length()) {
                fileLength = parent.length();
                filer = parent.reader(filer, fileLength, 1024 * 1024); //TODO config
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
                                LOG.warn("Truncating due to corruption while scanning");
                                return truncate(preTruncationNotifier, offsetFp);
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
                                LOG.warn("Truncating due to head-tail length mismatch while scanning");
                                return truncate(preTruncationNotifier, offsetFp);
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
                        try {
                            if (!stream.row(rowFP, rowTxId, rowType, row)) {
                                return false;
                            }
                        } catch (IOException e) {
                            if (allowRepairs) {
                                LOG.warn("Encountered I/O exception while streaming rows, we need to truncate", e);
                                return truncate(preTruncationNotifier, rowFP);
                            } else {
                                throw e;
                            }
                        }
                    }
                }
            }
            return true;
        } finally {
            ioStats.read.addAndGet(read);
        }
    }

    private boolean truncate(PreTruncationNotifier preTruncationNotifier, long offsetFp) throws Exception {
        if (preTruncationNotifier != null) {
            preTruncationNotifier.truncated(offsetFp);
        }
        synchronized (parent.lock()) {
            long before = parent.length();
            parent.truncate(offsetFp);
            LOG.warn("Truncated WAL at {}, before={} after={} {}", offsetFp, before, parent.length(), parent);
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
            truncate(null, parent.length() - numBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void close() throws IOException {
        parent.close();
    }
}
