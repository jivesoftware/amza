package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.RawConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.RawNextPointer;
import com.jivesoftware.os.amza.lsm.api.RawPointGet;
import com.jivesoftware.os.amza.lsm.api.RawPointerStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class PointerIndexNGTest {

    @Test(enabled = true)
    public void testLeapDisk() throws Exception {
        File indexFiler = File.createTempFile("l-index", ".tmp");

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 100;
        int step = 10;

        DiskBackedLeapPointerIndex pointerIndex = new DiskBackedLeapPointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
            64, 10);

        PointerIndexUtils.append(new Random(), pointerIndex, 0, step, count, desired);
        assertions(pointerIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemory() throws Exception {

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        RawMemoryPointerIndex walIndex = new RawMemoryPointerIndex(new SimpleRawEntry());

        PointerIndexUtils.append(new Random(), walIndex, 0, step, count, desired);
        assertions(walIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemoryToDisk() throws Exception {

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        RawMemoryPointerIndex memoryIndex = new RawMemoryPointerIndex(new SimpleRawEntry());

        PointerIndexUtils.append(new Random(), memoryIndex, 0, step, count, desired);
        assertions(memoryIndex, count, step, desired);

        File indexFiler = File.createTempFile("c-index", ".tmp");
        DiskBackedLeapPointerIndex disIndex = new DiskBackedLeapPointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
            64, 10);

        disIndex.append(memoryIndex);

        assertions(disIndex, count, step, desired);

    }

    private void assertions(RawConcurrentReadablePointerIndex walIndex, int count, int step, ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        RawNextPointer rowScan = walIndex.rawConcurrent(1024).rowScan();
        RawPointerStream stream = (rawEntry, offset, length) -> {
            System.out.println("rowScan:" + SimpleRawEntry.key(rawEntry));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawEntry.key(rawEntry));
            index[0]++;
            return true;
        };
        while (rowScan.next(stream));

        System.out.println("Point Get");
        for (int i = 0; i < count * step; i++) {
            long k = i;
            RawPointGet getPointer = walIndex.rawConcurrent(0).getPointer();
            stream = (rawEntry, offset, length) -> {

                System.out.println("Got: "+SimpleRawEntry.toString(rawEntry));
                if (SimpleRawEntry.value(rawEntry) != -1) {
                    byte[] d = desired.get(UIO.longBytes(SimpleRawEntry.key(rawEntry)));
                    if (d == null) {
                        Assert.assertTrue(d == null && SimpleRawEntry.value(rawEntry) == -1);
                    } else {
                        Assert.assertEquals(SimpleRawEntry.value(d), SimpleRawEntry.value(rawEntry));
                    }
                }
                return SimpleRawEntry.value(rawEntry) != -1;
            };

            while (getPointer.next(UIO.longBytes(k), stream));
        }

        System.out.println("Ranges");
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (entry, offset, length) -> {
                if (SimpleRawEntry.value(entry) > -1) {
                    System.out.println("Streamed:" + SimpleRawEntry.toString(entry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            RawNextPointer rangeScan = walIndex.rawConcurrent(1024).rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (entry, offset, length) -> {
                if (SimpleRawEntry.value(entry) > -1) {
                    streamed[0]++;
                }
                return SimpleRawEntry.value(entry) != -1;
            };
            RawNextPointer rangeScan = walIndex.rawConcurrent(1024).rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(2, streamed[0]);

        }
    }
}
