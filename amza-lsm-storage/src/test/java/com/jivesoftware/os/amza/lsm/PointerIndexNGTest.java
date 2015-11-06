package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.ConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerStream;
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

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 100;
        int step = 10;

        DiskBackedLeapPointerIndex pointerIndex = new DiskBackedLeapPointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
            64, 10);

        PointerIndexUtils.append(new Random(), pointerIndex, 0, step, count, desired);
        assertions(pointerIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemory() throws Exception {

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        MemoryPointerIndex walIndex = new MemoryPointerIndex();

        PointerIndexUtils.append(new Random(), walIndex, 0, step, count, desired);
        assertions(walIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemoryToDisk() throws Exception {

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        MemoryPointerIndex memoryIndex = new MemoryPointerIndex();

        PointerIndexUtils.append(new Random(), memoryIndex, 0, step, count, desired);
        assertions(memoryIndex, count, step, desired);

        File indexFiler = File.createTempFile("c-index", ".tmp");
        DiskBackedLeapPointerIndex disIndex = new DiskBackedLeapPointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
            64, 10);

        disIndex.append(memoryIndex);

        assertions(disIndex, count, step, desired);

    }

    private void assertions(ConcurrentReadablePointerIndex walIndex, int count, int step, ConcurrentSkipListMap<byte[], TimestampedValue> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        NextPointer rowScan = walIndex.concurrent(1024).rowScan();
        PointerStream stream = (key, timestamp, tombstoned, version, fp) -> {
            System.out.println("rowScan:" + UIO.bytesLong(key));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), UIO.bytesLong(key));
            index[0]++;
            return true;
        };
        while (rowScan.next(stream));

        System.out.println("Point Get");
        for (int i = 0; i < count * step; i++) {
            long k = i;
            NextPointer getPointer = walIndex.concurrent(0).getPointer(UIO.longBytes(k));
            stream = (key, timestamp, tombstoned, version, fp) -> {

                System.out.println(
                    "Got: Key=" + k + " Value= " + UIO.bytesLong(key) + " " + timestamp + " " + tombstoned + " " + version + " " + fp);
                if (fp != -1) {
                    TimestampedValue d = desired.get(key);
                    if (d == null) {
                        Assert.assertTrue(d == null && fp == -1);
                    } else {
                        Assert.assertEquals(UIO.bytesLong(d.getValue()), fp);
                    }
                }
                return fp != -1;
            };

            while (getPointer.next(stream));
        }

        System.out.println("Ranges");
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (key, timestamp, tombstoned, version, fp) -> {
                if (fp > -1) {
                    System.out.println("Streamed:" + UIO.bytesLong(key));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            NextPointer rangeScan = walIndex.concurrent(1024).rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (key, timestamp, tombstoned, version, fp) -> {
                if (fp > -1) {
                    streamed[0]++;
                }
                return fp != -1;
            };
            NextPointer rangeScan = walIndex.concurrent(1024).rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(2, streamed[0]);

        }
    }
}
