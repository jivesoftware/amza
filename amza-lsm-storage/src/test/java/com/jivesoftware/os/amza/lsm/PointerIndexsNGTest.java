package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class PointerIndexsNGTest {

    @Test(enabled = true)
    public void testTx() throws Exception {

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 100;
        int indexes = 4;

        PointerIndexs indexs = new PointerIndexs();
        for (int wi = 0; wi < indexes; wi++) {

            File indexFiler = File.createTempFile("a-index-" + wi, ".tmp");
            File keysFile = File.createTempFile("a-keys-" + wi, ".tmp");

            PointerIndex walIndex = new PointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
                new DiskBackedPointerIndexFiler(keysFile.getAbsolutePath(), "rw", false));

            PointerIndexUtils.append(walIndex, 0, step, count, desired);
            indexs.append(walIndex);
        }

        //LsmMemoryWalIndex memoryWalIndex = new LsmMemoryWalIndex();
        //append(memoryWalIndex, step, count, timeProvider, desired);
        //indexs.append(memoryWalIndex);
        assertions(indexs, count, step, desired);

        indexs.merge(() -> {
            File indexFiler = File.createTempFile("a-index-merged", ".tmp");
            File keysFile = File.createTempFile("a-keys-merged", ".tmp");

            return new PointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
                new DiskBackedPointerIndexFiler(keysFile.getAbsolutePath(), "rw", false));
        });
    }

    private void assertions(PointerIndexs indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], TimestampedValue> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        NextPointer rowScan = indexs.rowScan();
        PointerStream stream = (sortIndex, key, timestamp, tombstoned, fp) -> {
            //System.out.println(UIO.bytesLong(keys.get(index[0]))+" "+UIO.bytesLong(key));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), UIO.bytesLong(key));
            index[0]++;
            return true;
        };
        while (rowScan.next(stream));

        System.out.println("rowScan PASSED");

        for (int i = 0; i < count * step; i++) {
            long k = i;
            NextPointer getPointer = indexs.getPointer(UIO.longBytes(k));
            stream = (sortIndex, key, timestamp, tombstoned, fp) -> {
                TimestampedValue expectedFP = desired.get(key);
                if (expectedFP == null) {
                    Assert.assertTrue(expectedFP == null && fp == -1);
                } else {
                    Assert.assertEquals(UIO.bytesLong(expectedFP.getValue()), fp);
                }
                return true;
            };

            while (getPointer.next(stream));
        }

        System.out.println("getPointer PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (sortIndex, key, timestamp, tombstoned, fp) -> {
                if (fp > -1) {
                    System.out.println("Streamed:" + UIO.bytesLong(key));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            NextPointer rangeScan = indexs.rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        System.out.println("rangeScan PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (sortIndex, key, timestamp, tombstoned, fp) -> {
                if (fp > -1) {
                    streamed[0]++;
                }
                return true;
            };
            NextPointer rangeScan = indexs.rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(2, streamed[0]);

        }

        System.out.println("rangeScan2 PASSED");
    }

}
