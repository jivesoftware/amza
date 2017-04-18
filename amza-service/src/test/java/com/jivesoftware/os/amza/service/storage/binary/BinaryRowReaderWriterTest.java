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

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALWriter;
import com.jivesoftware.os.amza.service.filer.HeapByteBufferFactory;
import com.jivesoftware.os.amza.service.filer.MultiAutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.amza.api.IoStats;
import com.jivesoftware.os.amza.service.storage.filer.DiskBackedWALFiler;
import com.jivesoftware.os.amza.service.storage.filer.MemoryBackedWALFiler;
import com.jivesoftware.os.amza.service.storage.filer.WALFiler;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableInt;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class BinaryRowReaderWriterTest {

    private final WALWriter.IndexableKeys indexableKeys = stream -> true;
    private final WALWriter.TxKeyPointerFpStream txKeyPointerFpStream = (txId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, fp) -> true;

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
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer);

        binaryRowWriter.write(ioStats, 0L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 4 }), indexableKeys, txKeyPointerFpStream, true,
            false);
        binaryRowWriter.write(ioStats, 1L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 5 }), indexableKeys, txKeyPointerFpStream, true,
            false);
        binaryRowWriter.write(ioStats, 2L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 6 }), indexableKeys, txKeyPointerFpStream, true,
            false);
        binaryRowWriter.write(ioStats, -1L, RowType.end_of_merge, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 7 }), indexableKeys, txKeyPointerFpStream,
            true,
            false);

        MutableInt truncations = new MutableInt();
        binaryRowReader.validate(ioStats, true, true,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? rowFP : -1,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? -(rowFP + 1) : -1,
            (truncatedAtFP) -> truncations.increment());
        binaryRowReader.validate(ioStats, false, false,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? rowFP : -1,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? -(rowFP + 1) : -1,
            (truncatedAtFP) -> truncations.increment());
        Assert.assertEquals(truncations.intValue(), 0);

        //filer.seek(filer.length());
        long corruptionOffset = filer.length();
        UIO.writeByte(filer.appender(), (byte) 56, "corrupt");

        binaryRowReader.validate(ioStats, true, true,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? rowFP : -1,
            (long rowFP, long rowTxId, RowType rowType, byte[] row) -> rowType == RowType.end_of_merge ? -(rowFP + 1) : -1,
            (truncatedAtFP) -> {
                Assert.assertEquals(truncatedAtFP, corruptionOffset);
                truncations.increment();
            });
        Assert.assertEquals(truncations.intValue(), 1);

        UIO.writeByte(filer.appender(), (byte) 56, "corrupt");

        binaryRowReader.validate(ioStats, false, false,
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
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer);

        ReadStream readStream = new ReadStream();
        binaryRowReader.reverseScan(ioStats, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowReader.scan(ioStats, 0, false, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowWriter.write(ioStats, 0L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 1, 2, 3, 4 }), indexableKeys, txKeyPointerFpStream, true,
            false);
        binaryRowReader.scan(ioStats, 0, false, readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowReader.reverseScan(ioStats, readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowWriter.write(ioStats, 2L, RowType.primary, 1, 4, stream -> stream.stream(new byte[] { 2, 3, 4, 5 }), indexableKeys, txKeyPointerFpStream, true,
            false);
        binaryRowReader.scan(ioStats, 0, false, readStream);
        Assert.assertEquals(readStream.rows.size(), 2);
        readStream.clear();

        binaryRowReader.reverseScan(ioStats, readStream);
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
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer);

        ReadStream readStream = new ReadStream();
        binaryRowReader.reverseScan(ioStats, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        int numEntries = 1_000_000;
        for (long i = 0; i < numEntries; i++) {
            byte[] row = (String.valueOf(i) + "k" + (i % 1000)).getBytes();
            binaryRowWriter.write(ioStats, i, RowType.primary, 1, row.length, stream -> stream.stream(row), indexableKeys, txKeyPointerFpStream, true, false);
        }

        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();
            binaryRowReader.scan(ioStats, 0, false, readStream);
            System.out.println("Forward scan in " + (System.currentTimeMillis() - start) + " ms");
            Assert.assertEquals(readStream.rows.size(), numEntries);
            readStream.clear();

            start = System.currentTimeMillis();
            binaryRowReader.reverseScan(ioStats, readStream);
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
            BinaryRowReader binaryRowReader = new BinaryRowReader(filer);
            BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer);

            ReadStream readStream = new ReadStream();

            if (i > 0) {
                binaryRowReader.reverseScan(ioStats, readStream);
                Assert.assertEquals(readStream.rows.size(), i);
            }
            readStream.clear();

            byte[] row = new byte[4];
            rand.nextBytes(row);
            binaryRowWriter.write(ioStats, i, RowType.primary, 1, row.length, stream -> stream.stream(row), indexableKeys, txKeyPointerFpStream, true, false);
            filer.close();
        }

    }

    @Test(enabled = false)
    public void testConcurrency() throws Exception {
        MemoryBackedWALFiler walFiler = new MemoryBackedWALFiler(new MultiAutoGrowingByteBufferBackedFiler(32, 1_024 * 1_024, new HeapByteBufferFactory()));
        IoStats ioStats = new IoStats();
        BinaryRowReader binaryRowReader = new BinaryRowReader(walFiler);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(walFiler);

        ExecutorService executors = Executors.newFixedThreadPool(9);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong scanned = new AtomicLong();
        List<Future<?>> futures = Lists.newArrayList();
        for (int i = 0; i < 8; i++) {
            futures.add(executors.submit(() -> {
                try {
                    while (running.get()) {
                        binaryRowReader.scan(ioStats, 0, false, (rowFP, rowTxId, rowType, row) -> {
                            scanned.incrementAndGet();
                            return true;
                        });
                    }
                    return true;
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                }
            }));
        }
        futures.add(executors.submit(() -> {
            try {
                for (int i = 0; i < 1_000_000; i++) {
                    byte[] row = UIO.intBytes(i);
                    binaryRowWriter.write(ioStats, i, RowType.primary, 1, 16,
                        stream -> stream.stream(row),
                        stream -> true,
                        (txId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, fp) -> true,
                        false,
                        false);
                    if (i % 10_000 == 0) {
                        System.out.println("Finished i:" + i + " scanned:" + scanned.get());
                    }
                }
            } finally {
                running.set(false);
            }
            return null;
        }));

        for (Future<?> future : futures) {
            future.get();
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
