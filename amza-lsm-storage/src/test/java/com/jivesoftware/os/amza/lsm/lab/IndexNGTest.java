package com.jivesoftware.os.amza.lsm.lab;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class IndexNGTest {

    @Test(enabled = true)
    public void testLeapDisk() throws Exception {
        File indexFiler = File.createTempFile("l-index", ".tmp");

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 100;
        int step = 10;

        LeapsAndBoundsIndex pointerIndex = new LeapsAndBoundsIndex(new IndexFile(indexFiler.getAbsolutePath(), "rw", false),
            64, 10);

        IndexTestUtils.append(new Random(), pointerIndex, 0, step, count, desired);
        assertions(pointerIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemory() throws Exception {

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        RawMemoryIndex walIndex = new RawMemoryIndex(new SimpleRawEntry());

        IndexTestUtils.append(new Random(), walIndex, 0, step, count, desired);
        assertions(walIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemoryToDisk() throws Exception {

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        RawMemoryIndex memoryIndex = new RawMemoryIndex(new SimpleRawEntry());

        IndexTestUtils.append(new Random(), memoryIndex, 0, step, count, desired);
        assertions(memoryIndex, count, step, desired);

        File indexFiler = File.createTempFile("c-index", ".tmp");
        LeapsAndBoundsIndex disIndex = new LeapsAndBoundsIndex(new IndexFile(indexFiler.getAbsolutePath(), "rw", false),
            64, 10);

        disIndex.append(memoryIndex);

        assertions(disIndex, count, step, desired);

    }

    private void assertions(RawConcurrentReadableIndex walIndex, int count, int step, ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        NextRawEntry rowScan = walIndex.rawConcurrent(1024).rowScan();
        RawEntryStream stream = (rawEntry, offset, length) -> {
            System.out.println("rowScan:" + SimpleRawEntry.key(rawEntry));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawEntry.key(rawEntry));
            index[0]++;
            return true;
        };
        while (rowScan.next(stream));

        System.out.println("Point Get");
        for (int i = 0; i < count * step; i++) {
            long k = i;
            GetRaw getPointer = walIndex.rawConcurrent(0).get();
            byte[] key = UIO.longBytes(k);
            stream = (rawEntry, offset, length) -> {

                System.out.println("Got: "+SimpleRawEntry.toString(rawEntry));
                if (rawEntry != null) {
                    byte[] rawKey = UIO.longBytes(SimpleRawEntry.key(rawEntry));
                    Assert.assertEquals(rawKey, key);
                    byte[] d = desired.get(key);
                    if (d == null) {
                        Assert.fail();
                    } else {
                        Assert.assertEquals(SimpleRawEntry.value(rawEntry), SimpleRawEntry.value(d));
                    }
                } else {
                    Assert.assertFalse(desired.containsKey(key));
                }
                return rawEntry != null;
            };

            Assert.assertEquals(getPointer.get(key, stream), desired.containsKey(key));
        }

        System.out.println("Ranges");
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (entry, offset, length) -> {
                if (entry != null) {
                    System.out.println("Streamed:" + SimpleRawEntry.toString(entry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            NextRawEntry rangeScan = walIndex.rawConcurrent(1024).rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (entry, offset, length) -> {
                if (entry != null) {
                    streamed[0]++;
                }
                return SimpleRawEntry.value(entry) != -1;
            };
            NextRawEntry rangeScan = walIndex.rawConcurrent(1024).rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(2, streamed[0]);

        }
    }
}
