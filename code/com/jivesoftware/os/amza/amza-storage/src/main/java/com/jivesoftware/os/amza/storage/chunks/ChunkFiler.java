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
package com.jivesoftware.os.amza.storage.chunks;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;

public class ChunkFiler {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static String name(String _chunkName) {
        String fileName = _chunkName + ".chunk";
        return fileName;
    }

    private static void ensureDirectory(File _file) {
        if (!_file.exists()) {
            File parent = _file.getParentFile();
            if (parent != null) {
                parent.mkdirs();
            }
        }
    }

    // kill these statics in favor of a provider
    public static ChunkFiler factory(File dir, String _chunkName) throws Exception {
        File file = new File(dir, name(_chunkName));
        if (file.exists()) {
            return openInstance(dir, _chunkName);
        }
        return newInstance(dir, _chunkName);
    }

    public static ChunkFiler newInstance(File dir, String _chunkName) throws Exception {
        File file = new File(dir, name(_chunkName));
        ensureDirectory(file);

        Filer chunkFiler = Filer.open(file, "rw");
        SubFilers chunkSegmenter = new SubFilers(file, chunkFiler);
        SubFiler chunkSegment = chunkSegmenter.get(0, Long.MAX_VALUE, 0);
        ChunkFiler chunks = new ChunkFiler();
        chunks.open(chunkSegment);
        return chunks;
    }

    public static ChunkFiler openInstance(File dir, String _chunkName) throws Exception {
        File file = new File(dir, name(_chunkName));
        Filer chunkFiler = Filer.open(file, "rw");
        SubFilers chunkSegmenter = new SubFilers(file, chunkFiler);
        SubFiler chunkSegment = chunkSegmenter.get(0, Long.MAX_VALUE, 0);
        ChunkFiler chunks = new ChunkFiler(chunkSegment);
        chunks.open();
        return chunks;
    }

    private static final long cMagicNumber = Long.MAX_VALUE;
    private static final int cMinPower = 8;
    private long lengthOfFile = 8 + 8 + (8 * (64 - cMinPower));
    private long referenceNumber = 0;
    private SubFiler filer;

    /*
     New Call Sequence
     ChunkFiler chunks = ChunkFiler();
     chunks.setup(100);
     open(_filer);
     */
    public ChunkFiler() {
    }

    /*
     * file header format
     * lengthOfFile
     * referenceNumber
     * free 2^8
     * free 2^9
     * thru
     * free 2^64
     */
    public void setup(long _referenceNumber) {
        lengthOfFile = 8 + 8 + (8 * (64 - cMinPower));
        referenceNumber = _referenceNumber;
    }

    public long bytesNeeded() {
        return Long.MAX_VALUE;
    }

    public void open(SubFiler _filer) throws Exception {
        filer = _filer;
        synchronized (filer.lock()) {
            UIO.writeLong(filer, lengthOfFile, "lengthOfFile");
            UIO.writeLong(filer, referenceNumber, "referenceNumber");
            for (int i = cMinPower; i < 65; i++) {
                UIO.writeLong(filer, -1, "free");
            }
            filer.flush();
        }
    }

    /*
     Exsisting Call Sequence
     ChunkFiler chunks = ChunkFiler(_filer);
     open();
     */
    public ChunkFiler(SubFiler _filer) throws Exception {
        filer = _filer;
    }

    public void open() throws Exception {
        synchronized (filer.lock()) {
            filer.seek(0);
            lengthOfFile = UIO.readLong(filer, "lengthOfFile");
            referenceNumber = UIO.readLong(filer, "referenceNumber");
        }
    }

    public void close() throws IOException {
        filer.close();
    }

    public long getReferenceNumber() {
        return referenceNumber;
    }

    public void allChunks(ICallback<Long, Long> _chunks) throws Exception {
        synchronized (filer.lock()) {
            filer.seek(8 + 8 + (8 * (64 - cMinPower)));
            long size = filer.getSize();
            while (filer.getFilePointer() < size) {
                long chunkFP = filer.getFilePointer();
                long magicNumber = UIO.readLong(filer, "magicNumber");
                if (magicNumber != cMagicNumber) {
                    throw new Exception("Invalid chunkFP " + chunkFP);
                }
                long chunkPower = UIO.readLong(filer, "chunkPower");
                UIO.readLong(filer, "chunkNexFreeChunkFP");
                long chunkLength = UIO.readLong(filer, "chunkLength");
                long fp = filer.getFilePointer();
                if (chunkLength > 0) {
                    long more = _chunks.callback(chunkFP);
                    if (more != chunkFP) {
                        break;
                    }
                }
                filer.seek(fp + UIO.chunkLength(chunkPower));
            }
        }
    }

    public long newChunk(long _chunkLength) throws Exception {
        long resultFP;
        synchronized (filer.lock()) {
            long chunkPower = UIO.chunkPower(_chunkLength, cMinPower);
            resultFP = resuseChunk(chunkPower);
            if (resultFP == -1) {
                long chunkLength = UIO.chunkLength(chunkPower);
                chunkLength += 8; // add magicNumber
                chunkLength += 8; // add chunkPower
                chunkLength += 8; // add next free chunk of equal size
                chunkLength += 8; // add bytesLength
                long newChunkFP = lengthOfFile;
                if (newChunkFP + chunkLength > filer.endOfFP()) {
                    //!! to do over flow allocated chunk request reallocation
                    throw new RuntimeException("need larger allocation for ChunkFile" + this);
                }
                synchronized (filer.lock()) {
                    filer.seek(newChunkFP + chunkLength - 1); // last byte in chunk
                    filer.write(0); // cause file backed ChunkFiler to grow file on disk
                    filer.seek(newChunkFP);
                    UIO.writeLong(filer, cMagicNumber, "magicNumber");
                    UIO.writeLong(filer, chunkPower, "chunkPower");
                    UIO.writeLong(filer, -1, "chunkNexFreeChunkFP");
                    UIO.writeLong(filer, _chunkLength, "chunkLength");
                    lengthOfFile += chunkLength;
                    filer.seek(0);
                    UIO.writeLong(filer, lengthOfFile, "lengthOfFile");
                    filer.flush();
                }
                return newChunkFP;
            }
        }
        return resultFP;
    }

    private long resuseChunk(long _chunkPower) throws Exception {
        synchronized (filer.lock()) {
            filer.seek(freeSeek(_chunkPower));
            long reuseFP = UIO.readLong(filer, "free");
            if (reuseFP == -1) {
                return reuseFP;
            }
            long nextFree = readNextFree(reuseFP);
            filer.seek(freeSeek(_chunkPower));
            UIO.writeLong(filer, nextFree, "free");
            return reuseFP;
        }
    }

    public SubFiler getFiler(long _chunkFP) throws Exception {
        long chunkPower = 0;
        long nextFreeChunkFP = 0;
        long length = 0;
        long fp = 0;
        synchronized (filer.lock()) {
            filer.seek(_chunkFP);
            long magicNumber = UIO.readLong(filer, "magicNumber");
            if (magicNumber != cMagicNumber) {
                throw new Exception("Invalid chunkFP:" + _chunkFP
                        + " excountered bad magicNumber:" + magicNumber
                        + " expected:" + cMagicNumber);
            }
            chunkPower = UIO.readLong(filer, "chunkPower");
            nextFreeChunkFP = UIO.readLong(filer, "chunkNexFreeChunkFP");
            length = UIO.readLong(filer, "chunkLength");
            fp = filer.getFilePointer();
        }

        try {
            return filer.get(fp, fp + UIO.chunkLength(chunkPower), length);
        } catch (Exception x) {
            LOG.error("Failed to get SubFiler for chunkFP:" + _chunkFP, x);
            LOG.error("_chunkFP=" + _chunkFP
                    + "nextFree=" + nextFreeChunkFP
                    + "fp=" + fp
                    + "length=" + length
                    + "chunkPower=" + chunkPower);
            throw x;
        }
    }

    public void remove(long _chunkFP) throws Exception {
        synchronized (filer.lock()) {
            filer.seek(_chunkFP);
            long magicNumber = UIO.readLong(filer, "magicNumber");
            if (magicNumber != cMagicNumber) {
                throw new Exception("Invalid chunkFP " + _chunkFP);
            }
            long chunkPower = UIO.readLong(filer, "chunkPower");
            UIO.readLong(filer, "chunkNexFreeChunkFP");
            UIO.writeLong(filer, -1, "chunkLength");
            long chunkLength = UIO.chunkLength(chunkPower); // bytes
            // fill with zeros
            while (chunkLength >= zerosMax.length) {
                filer.write(zerosMax);
                chunkLength -= zerosMax.length;
            }
            while (chunkLength >= zerosMin.length) {
                filer.write(zerosMin);
                chunkLength -= zerosMin.length;
            }
            filer.flush();
            // save as free chunk
            filer.seek(freeSeek(chunkPower));
            long freeFP = UIO.readLong(filer, "free");
            if (freeFP == -1) {
                filer.seek(freeSeek(chunkPower));
                UIO.writeLong(filer, _chunkFP, "free");
                filer.flush();
            } else {
                long nextFree = readNextFree(freeFP);
                filer.seek(freeSeek(chunkPower));
                UIO.writeLong(filer, _chunkFP, "free");
                writeNextFree(_chunkFP, nextFree);
                filer.flush();
            }
        }
    }
    private static final byte[] zerosMin = new byte[(int) Math.pow(2, cMinPower)]; // never too big
    private static final byte[] zerosMax = new byte[(int) Math.pow(2, 16)]; // 65536 max used until min needed

    private long freeSeek(long _chunkPower) {
        return 8 + 8 + ((_chunkPower - cMinPower) * 8);
    }

    private long readNextFree(long _chunkFP) throws Exception {
        synchronized (filer.lock()) {
            filer.seek(_chunkFP);
            UIO.readLong(filer, "magicNumber");
            UIO.readLong(filer, "chunkPower");
            return UIO.readLong(filer, "chunkNexFreeChunkFP");
        }
    }

    private void writeNextFree(long _chunkFP, long _nextFreeFP) throws Exception {
        synchronized (filer.lock()) {
            filer.seek(_chunkFP);
            UIO.readLong(filer, "magicNumber");
            UIO.readLong(filer, "chunkPower");
            UIO.writeLong(filer, _nextFreeFP, "chunkNexFreeChunkFP");
        }
    }
}