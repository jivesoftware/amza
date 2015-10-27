package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.ConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerStream;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class PointerIndexNGTest {

    @Test(enabled = true)
    public void testDisk() throws Exception {
        File indexFiler = File.createTempFile("b-index", ".tmp");
        File keysFile = File.createTempFile("b-keys", ".tmp");

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        DiskBackedPointerIndex pointerIndex = new DiskBackedPointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
            new DiskBackedPointerIndexFiler(keysFile.getAbsolutePath(), "rw", false));

        PointerIndexUtils.append(pointerIndex, 0, step, count, desired);
        assertions(pointerIndex, count, step, desired);
    }

    @Test(enabled = true)
    public void testMemory() throws Exception {

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        MemoryPointerIndex walIndex = new MemoryPointerIndex();

        PointerIndexUtils.append(walIndex, 0, step, count, desired);
        assertions(walIndex, count, step, desired);
    }

    @Test(enabled = true)
    public void testMemoryToDisk() throws Exception {

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        MemoryPointerIndex memoryIndex = new MemoryPointerIndex();

        PointerIndexUtils.append(memoryIndex, 0, step, count, desired);
        assertions(memoryIndex, count, step, desired);

        File indexFiler = File.createTempFile("c-index", ".tmp");
        File keysFile = File.createTempFile("c-keys", ".tmp");
        DiskBackedPointerIndex disIndex = new DiskBackedPointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
            new DiskBackedPointerIndexFiler(keysFile.getAbsolutePath(), "rw", false));

        disIndex.append(memoryIndex);

        assertions(disIndex, count, step, desired);

    }

    private void assertions(ConcurrentReadablePointerIndex walIndex, int count, int step, ConcurrentSkipListMap<byte[], TimestampedValue> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        NextPointer rowScan = walIndex.concurrent().rowScan();
        PointerStream stream = (sortIndex, key, timestamp, tombstoned, version, fp) -> {
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), UIO.bytesLong(key));
            index[0]++;
            return true;
        };

        while (rowScan.next(stream));

        for (int i = 0; i < count * step; i++) {
            long k = i;
            NextPointer getPointer = walIndex.concurrent().getPointer(UIO.longBytes(k));
            stream = (sortIndex, key, timestamp, tombstoned, version, fp) -> {
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

            while (getPointer.next(stream)) {
                System.out.println("k:" + k + " " + System.currentTimeMillis());
            }
        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (sortIndex, key, timestamp, tombstoned, version, fp) -> {
                if (fp > -1) {
                    System.out.println("Streamed:" + UIO.bytesLong(key));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            NextPointer rangeScan = walIndex.concurrent().rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (sortIndex, key, timestamp, tombstoned, version, fp) -> {
                if (fp > -1) {
                    streamed[0]++;
                }
                return fp != -1;
            };
            NextPointer rangeScan = walIndex.concurrent().rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(2, streamed[0]);

        }
    }
}
