package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.service.storage.filer.DiskBackedWALFiler;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LsmWalIndexNGTest {

    @Test
    public void testDisk() throws Exception {
        File indexFiler = File.createTempFile("index", ".tmp");
        File keysFile = File.createTempFile("keys", ".tmp");

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        LsmWalIndex walIndex = new LsmWalIndex(new DiskBackedWALFiler(indexFiler.getAbsolutePath(), "rw", false),
            new DiskBackedWALFiler(keysFile.getAbsolutePath(), "rw", false));

        LsmWalIndexUtils.append(walIndex, step, count, desired);
        assertions(walIndex, count, step, desired);
    }

    @Test
    public void testMemory() throws Exception {

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        LsmMemoryWalIndex walIndex = new LsmMemoryWalIndex();

        LsmWalIndexUtils.append(walIndex, step, count, desired);
        assertions(walIndex, count, step, desired);
    }

    @Test
    public void testMemoryToDisk() throws Exception {

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 10;
        int step = 10;

        LsmMemoryWalIndex memoryIndex = new LsmMemoryWalIndex();

        LsmWalIndexUtils.append(memoryIndex, step, count, desired);
        assertions(memoryIndex, count, step, desired);

        File indexFiler = File.createTempFile("index", ".tmp");
        File keysFile = File.createTempFile("keys", ".tmp");
        LsmWalIndex disIndex = new LsmWalIndex(new DiskBackedWALFiler(indexFiler.getAbsolutePath(), "rw", false),
            new DiskBackedWALFiler(keysFile.getAbsolutePath(), "rw", false));

        disIndex.append(memoryIndex);

        assertions(memoryIndex, count, step, desired);

    }

    private void assertions(ConcurrentReadableWalIndex walIndex, int count, int step, ConcurrentSkipListMap<byte[], TimestampedValue> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        FeedNext rowScan = walIndex.concurrent().rowScan();
        WALKeyPointerStream stream = (key, timestamp, tombstoned, fp) -> {
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), UIO.bytesLong(key));
            index[0]++;
            return true;
        };

        while (rowScan.feedNext(stream));

        for (int i = 0; i < count * step; i++) {
            long k = i;
            FeedNext getPointer = walIndex.concurrent().getPointer(UIO.longBytes(k));
            stream = (key, timestamp, tombstoned, fp) -> {
                TimestampedValue d = desired.get(key);
                if (d == null) {
                    Assert.assertTrue(d == null && fp == -1);
                } else {
                    Assert.assertEquals(UIO.bytesLong(d.getValue()), fp);
                }
                return true;
            };

            while (getPointer.feedNext(stream));
        }

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
            FeedNext rangeScan = walIndex.concurrent().rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.feedNext(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (key, timestamp, tombstoned, fp) -> {
                if (fp > -1) {
                    streamed[0]++;
                }
                return true;
            };
            FeedNext rangeScan = walIndex.concurrent().rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.feedNext(stream));
            Assert.assertEquals(2, streamed[0]);

        }
    }
}
