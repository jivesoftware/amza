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

import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALWriter;
import com.jivesoftware.os.amza.service.filer.HeapByteBufferFactory;
import com.jivesoftware.os.amza.service.filer.MultiAutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.amza.service.stats.IoStats;
import com.jivesoftware.os.amza.service.storage.filer.DiskBackedWALFiler;
import com.jivesoftware.os.amza.service.storage.filer.MemoryBackedWALFiler;
import com.jivesoftware.os.amza.service.storage.filer.WALFiler;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.lang.mutable.MutableInt;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class BinaryRowReaderWriterTest {

    private final WALWriter.IndexableKeys indexableKeys = stream -> true;
    private final WALWriter.TxKeyPointerFpStream txKeyPointerFpStream = (txId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> true;

    @Test
    public void testValidateDiskBacked() throws Exception {

        File dir = Files.createTempDir();
        IoStats ioStats = new IoStats();
        DiskBackedWALFiler filer = new DiskBackedWALFiler(new File(dir, "booya").getAbsolutePath(), "rw", false, 0);
        validate(filer, ioStats);

    }

    @Test
    public void testValidateHeapBacked() throws Exception {

        IoStats ioStats = new IoStats();
        WALFiler filer = new MemoryBackedWALFiler(new MultiAutoGrowingByteBufferBackedFiler(1_024, 1_024 * 1_024,
            new HeapByteBufferFactory()));
        validate(filer, ioStats);

    }

    private void validate(WALFiler filer, IoStats ioStats) throws Exception {
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer, ioStats);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer, ioStats);

        binaryRowWriter.write(0L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 4 }), indexableKeys, txKeyPointerFpStream, false);
        binaryRowWriter.write(1L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 5 }), indexableKeys, txKeyPointerFpStream, false);
        binaryRowWriter.write(2L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 6 }), indexableKeys, txKeyPointerFpStream, false);
        binaryRowWriter.write(-1L, RowType.end_of_merge, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 7 }), indexableKeys, txKeyPointerFpStream, false);

        MutableInt truncations = new MutableInt();
        binaryRowReader.validate(true,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? rowFP : -1,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? -(rowFP + 1) : -1,
            (truncatedAtFP) -> truncations.increment());
        binaryRowReader.validate(false,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? rowFP : -1,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? -(rowFP + 1) : -1,
            (truncatedAtFP) -> truncations.increment());
        Assert.assertEquals(truncations.intValue(), 0);

        //filer.seek(filer.length());
        long corruptionOffset = filer.length();
        UIO.writeByte(filer.appender(), (byte) 56, "corrupt");

        binaryRowReader.validate(true,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? rowFP : -1,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? -(rowFP + 1) : -1,
            (truncatedAtFP) -> {
                Assert.assertEquals(truncatedAtFP, corruptionOffset);
                truncations.increment();
            });
        Assert.assertEquals(truncations.intValue(), 1);

        UIO.writeByte(filer.appender(), (byte) 56, "corrupt");

        binaryRowReader.validate(false,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? rowFP : -1,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? -(rowFP + 1) : -1,
            (truncatedAtFP) -> {
                Assert.assertEquals(truncatedAtFP, corruptionOffset);
                truncations.increment();
            });
        Assert.assertEquals(truncations.intValue(), 2);
    }

    @Test
    public void testDiskBackedRead() throws Exception {
        File dir = Files.createTempDir();
        IoStats ioStats = new IoStats();
        DiskBackedWALFiler filer = new DiskBackedWALFiler(new File(dir, "booya").getAbsolutePath(), "rw", false, 0);
        read(filer, ioStats);

    }

    @Test
    public void testMemoryBackedRead() throws Exception {
        IoStats ioStats = new IoStats();
        WALFiler filer = new MemoryBackedWALFiler(new MultiAutoGrowingByteBufferBackedFiler(1_024, 1_024 * 1_024,
            new HeapByteBufferFactory()));
        read(filer, ioStats);

    }

    private void read(WALFiler filer, IoStats ioStats) throws Exception {
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer, ioStats);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer, ioStats);

        ReadStream readStream = new ReadStream();
        binaryRowReader.reverseScan(readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowReader.scan(0, false, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowWriter.write(0L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 4 }), indexableKeys, txKeyPointerFpStream, false);
        binaryRowReader.scan(0, false, readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowReader.reverseScan(readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowWriter.write(2L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 2, 3, 4, 5 }), indexableKeys, txKeyPointerFpStream, false);
        binaryRowReader.scan(0, false, readStream);
        Assert.assertEquals(readStream.rows.size(), 2);
        readStream.clear();

        binaryRowReader.reverseScan(readStream);
        Assert.assertEquals(readStream.rows.size(), 2);
        Assert.assertTrue(Arrays.equals(readStream.rows.get(0), new byte[] { 2, 3, 4, 5 }));
        Assert.assertTrue(Arrays.equals(readStream.rows.get(1), new byte[] { 1, 2, 3, 4 }));
        readStream.clear();
    }

    @Test(enabled = false)
    public void testReverseReadPerformance() throws Exception {
        File dir = Files.createTempDir();
        IoStats ioStats = new IoStats();
        DiskBackedWALFiler filer = new DiskBackedWALFiler(new File(dir, "booya").getAbsolutePath(), "rw", false, 0);
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer, ioStats);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer, ioStats);

        ReadStream readStream = new ReadStream();
        binaryRowReader.reverseScan(readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        int numEntries = 1_000_000;
        for (long i = 0; i < numEntries; i++) {
            byte[] row = (String.valueOf(i) + "k" + (i % 1000)).getBytes();
            binaryRowWriter.write(i, RowType.primary, 1, row.length, stream -> stream.stream(row), indexableKeys, txKeyPointerFpStream, false);
        }

        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();
            binaryRowReader.scan(0, false, readStream);
            System.out.println("Forward scan in " + (System.currentTimeMillis() - start) + " ms");
            Assert.assertEquals(readStream.rows.size(), numEntries);
            readStream.clear();

            start = System.currentTimeMillis();
            binaryRowReader.reverseScan(readStream);
            System.out.println("Reverse scan in " + (System.currentTimeMillis() - start) + " ms");
            Assert.assertEquals(readStream.rows.size(), numEntries);
            readStream.clear();
        }
    }

    @Test
    public void testOpenCloseAppend() throws Exception {
        File dir = Files.createTempDir();

        IoStats ioStats = new IoStats();
        Random rand = new Random();
        for (long i = 0; i < 1000; i++) {
            DiskBackedWALFiler filer = new DiskBackedWALFiler(new File(dir, "foo").getAbsolutePath(), "rw", false, 0);
            BinaryRowReader binaryRowReader = new BinaryRowReader(filer, ioStats);
            BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer, ioStats);

            ReadStream readStream = new ReadStream();

            if (i > 0) {
                binaryRowReader.reverseScan(readStream);
                Assert.assertEquals(readStream.rows.size(), i);
            }
            readStream.clear();

            byte[] row = new byte[4];
            rand.nextBytes(row);
            binaryRowWriter.write(i, RowType.primary, 1, row.length, stream -> stream.stream(row), indexableKeys, txKeyPointerFpStream, false);
            filer.close();
        }

    }

    static class ReadStream implements RowStream {

        int clears = 0;
        final ArrayList<byte[]> rows = new ArrayList<>();

        @Override
        public boolean row(long rowFp, long rwoTxId, RowType rowType, byte[] row) throws Exception {
            rows.add(row);
            return true;
        }

        void clear() {
            clears++;
            rows.clear();
        }
    }

}
