package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.service.storage.filer.DiskBackedWALFiler;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LsmWalIndexsNGTest {

    @Test
    public void testTx() throws Exception {

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 100;
        int indexes = 4;

        OrderIdProvider timeProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        LsmWalIndexs indexs = new LsmWalIndexs();
        for (int wi = 0; wi < indexes; wi++) {

            File indexFiler = File.createTempFile("index-" + wi, ".tmp");
            File keysFile = File.createTempFile("keys-" + wi, ".tmp");

            LsmWalIndex walIndex = new LsmWalIndex(new DiskBackedWALFiler(indexFiler.getAbsolutePath(), "rw", false),
                new DiskBackedWALFiler(keysFile.getAbsolutePath(), "rw", false));

            LsmWalIndexUtils.append(walIndex, step, count, desired);
            indexs.append(walIndex);
        }

        //LsmMemoryWalIndex memoryWalIndex = new LsmMemoryWalIndex();
        //append(memoryWalIndex, step, count, timeProvider, desired);
        //indexs.append(memoryWalIndex);
        assertions(indexs, count, step, desired);

        indexs.merge(() -> {
            File indexFiler = File.createTempFile("index-merged", ".tmp");
            File keysFile = File.createTempFile("keys-merged", ".tmp");

            return new LsmWalIndex(new DiskBackedWALFiler(indexFiler.getAbsolutePath(), "rw", false),
                new DiskBackedWALFiler(keysFile.getAbsolutePath(), "rw", false));
        });
    }

    private void assertions(LsmWalIndexs indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], TimestampedValue> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        FeedNext rowScan = indexs.rowScan();
        WALKeyPointerStream stream = (key, timestamp, tombstoned, fp) -> {
            //System.out.println(UIO.bytesLong(keys.get(index[0]))+" "+UIO.bytesLong(key));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), UIO.bytesLong(key));
            index[0]++;
            return true;
        };
        while (rowScan.feedNext(stream));

        System.out.println("rowScan PASSED");

        for (int i = 0; i < count * step; i++) {
            long k = i;
            FeedNext getPointer = indexs.getPointer(UIO.longBytes(k));
            stream = (key, timestamp, tombstoned, fp) -> {
                TimestampedValue expectedFP = desired.get(key);
                if (expectedFP == null) {
                    Assert.assertTrue(expectedFP == null && fp == -1);
                } else {
                    Assert.assertEquals(UIO.bytesLong(expectedFP.getValue()), fp);
                }
                return true;
            };

            while (getPointer.feedNext(stream));
        }

        System.out.println("getPointer PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (key, timestamp, tombstoned, fp) -> {
                if (fp > -1) {
                    System.out.println("Streamed:" + UIO.bytesLong(key));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            FeedNext rangeScan = indexs.rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.feedNext(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        System.out.println("rangeScan PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (key, timestamp, tombstoned, fp) -> {
                if (fp > -1) {
                    streamed[0]++;
                }
                return true;
            };
            FeedNext rangeScan = indexs.rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.feedNext(stream));
            Assert.assertEquals(2, streamed[0]);

        }

        System.out.println("rangeScan2 PASSED");
    }

}
