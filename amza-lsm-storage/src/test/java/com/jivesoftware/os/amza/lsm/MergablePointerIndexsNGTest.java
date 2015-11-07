package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.UIO;
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
public class MergablePointerIndexsNGTest {

    @Test(enabled = true)
    public void testTx() throws Exception {

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 100;
        int indexes = 4;

        MergeablePointerIndexs indexs = new MergeablePointerIndexs();
        Random rand = new Random();
        for (int wi = 0; wi < indexes; wi++) {

            File indexFiler = File.createTempFile("a-index-" + wi, ".tmp");

            DiskBackedLeapPointerIndex walIndex = new DiskBackedLeapPointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
                64, 2);

            PointerIndexUtils.append(rand, walIndex, 0, step, count, desired);
            indexs.append(walIndex);
        }

        assertions(indexs, count, step, desired);

        indexs.merge(2, () -> {
            File indexFiler = File.createTempFile("a-index-merged", ".tmp");

            return new DiskBackedLeapPointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false), 64, 2);
        }, (index) -> {
            return index;
        });

        assertions(indexs, count, step, desired);
    }

    private void assertions(MergeablePointerIndexs indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        RawNextPointer rowScan = indexs.rowScan();
        RawPointerStream stream = (rawEntry, offset, length) -> {
            System.out.println(UIO.bytesLong(keys.get(index[0])) + " " + SimpleRawEntry.toString(rawEntry));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawEntry.key(rawEntry));
            index[0]++;
            return true;
        };
        while (rowScan.next(stream));

        System.out.println("rowScan PASSED");

        for (int i = 0; i < count * step; i++) {
            long k = i;
            RawPointGet getPointer = indexs.getPointer();
            stream = (rawEntry, offset, length) -> {
                byte[] expected = desired.get(UIO.longBytes(SimpleRawEntry.key(rawEntry)));
                if (expected == null) {
                    System.out.println("expected:" + expected + " " + SimpleRawEntry.value(rawEntry));
                    Assert.assertTrue(expected == null && SimpleRawEntry.value(rawEntry) == 0);
                } else {
                    Assert.assertEquals(SimpleRawEntry.value(expected), SimpleRawEntry.value(rawEntry));
                }
                return true;
            };

            while (getPointer.next(UIO.longBytes(k), stream));
        }

        System.out.println("getPointer PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (rawEntry, offset, length) -> {
                if (SimpleRawEntry.value(rawEntry) > -1) {
                    System.out.println("Streamed:" + SimpleRawEntry.toString(rawEntry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            RawNextPointer rangeScan = indexs.rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        System.out.println("rangeScan PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (rawEntry, offset, length) -> {
                if (SimpleRawEntry.value(rawEntry) > -1) {
                    streamed[0]++;
                }
                return true;
            };
            RawNextPointer rangeScan = indexs.rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(2, streamed[0]);

        }

        System.out.println("rangeScan2 PASSED");
    }

}
