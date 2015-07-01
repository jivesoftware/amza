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
import com.jivesoftware.os.amza.service.storage.filer.DiskBackedWALFiler;
import com.jivesoftware.os.amza.service.storage.filer.MemoryBackedWALFiler;
import com.jivesoftware.os.amza.service.storage.filer.WALFiler;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class BinaryRowReaderWriterTest {

    @Test
    public void testValidateDiskBacked() throws Exception {

        File dir = Files.createTempDir();
        IoStats ioStats = new IoStats();
        DiskBackedWALFiler filer = new DiskBackedWALFiler(new File(dir, "booya").getAbsolutePath(), "rw", false);
        validate(filer, ioStats);

    }

    @Test
    public void testValidateHeapBacked() throws Exception {

        IoStats ioStats = new IoStats();
        WALFiler filer = new MemoryBackedWALFiler(new HeapFiler());
        validate(filer, ioStats);

    }

    private void validate(WALFiler filer, IoStats ioStats) throws IOException, Exception {
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer, ioStats, 3);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer, ioStats);

        binaryRowWriter.writePrimary(Collections.nCopies(1, 0L), Collections.singletonList(new byte[] { 1, 2, 3, 4 }));
        binaryRowWriter.writePrimary(Collections.nCopies(1, 1L), Collections.singletonList(new byte[] { 1, 2, 3, 5 }));
        binaryRowWriter.writePrimary(Collections.nCopies(1, 2L), Collections.singletonList(new byte[] { 1, 2, 3, 6 }));

        Assert.assertTrue(binaryRowReader.validate());

        filer.seek(filer.length());
        UIO.writeByte(filer, (byte) 56, "corrupt");

        Assert.assertFalse(binaryRowReader.validate());
    }

    @Test
    public void testDiskBackedRead() throws Exception {
        File dir = Files.createTempDir();
        IoStats ioStats = new IoStats();
        DiskBackedWALFiler filer = new DiskBackedWALFiler(new File(dir, "booya").getAbsolutePath(), "rw", false);
        read(filer, ioStats);

    }

    @Test
    public void testMemoryBackedRead() throws Exception {
        File dir = Files.createTempDir();
        IoStats ioStats = new IoStats();
        WALFiler filer = new MemoryBackedWALFiler(new HeapFiler());
        read(filer, ioStats);

    }

    private void read(WALFiler filer, IoStats ioStats) throws Exception {
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer, ioStats, 1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer, ioStats);

        ReadStream readStream = new ReadStream();
        binaryRowReader.reverseScan(readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowReader.scan(0, false, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowWriter.writePrimary(Collections.nCopies(1, 0L), Collections.singletonList(new byte[] { 1, 2, 3, 4 }));
        binaryRowReader.scan(0, false, readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowReader.reverseScan(readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowWriter.writePrimary(Collections.nCopies(1, 2L), Collections.singletonList(new byte[] { 2, 3, 4, 5 }));
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
        DiskBackedWALFiler filer = new DiskBackedWALFiler(new File(dir, "booya").getAbsolutePath(), "rw", false);
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer, ioStats, 1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer, ioStats);

        ReadStream readStream = new ReadStream();
        binaryRowReader.reverseScan(readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        int numEntries = 1_000_000;
        for (int i = 0; i < numEntries; i++) {
            binaryRowWriter.writePrimary(Collections.nCopies(1, (long) i), Collections.singletonList((String.valueOf(i) + "k" + (i % 1000)).getBytes()));
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
        for (int i = 0; i < 1000; i++) {
            DiskBackedWALFiler filer = new DiskBackedWALFiler(new File(dir, "foo").getAbsolutePath(), "rw", false);
            BinaryRowReader binaryRowReader = new BinaryRowReader(filer, ioStats, 1);
            BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer, ioStats);

            ReadStream readStream = new ReadStream();

            if (i > 0) {
                binaryRowReader.reverseScan(readStream);
                Assert.assertEquals(readStream.rows.size(), i);
            }
            readStream.clear();

            byte[] row = new byte[4];
            rand.nextBytes(row);
            binaryRowWriter.writePrimary(Collections.nCopies(1, (long) i), Arrays.asList(row));
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
