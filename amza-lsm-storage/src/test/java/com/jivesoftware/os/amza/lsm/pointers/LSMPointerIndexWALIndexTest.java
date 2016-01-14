package com.jivesoftware.os.amza.lsm.pointers;

import com.google.common.io.Files;
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.CompactionWALIndex;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeyStream;
import java.io.File;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class LSMPointerIndexWALIndexTest {

    @Test
    public void testPut() throws Exception {
        File dir0 = Files.createTempDir();
        VersionedPartitionName partitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()),
            VersionedPartitionName.STATIC_VERSION);
        LSMPointerIndexWALIndex index = getIndex(dir0, partitionName);
        index.merge(stream -> stream.stream(0L, UIO.longBytes(0), UIO.longBytes(0), System.currentTimeMillis(), false, Long.MAX_VALUE, 0L),
            null);
        index.merge(stream -> stream.stream(1L, UIO.longBytes(1), UIO.longBytes(1), System.currentTimeMillis(), false, Long.MAX_VALUE, 1L),
            null);

        index.getPointer(UIO.longBytes(1), UIO.longBytes(1), (prefix, key, timestamp, tombstoned, version, fp) -> {
            assertEquals(fp, 1);
            return true;
        });

        Assert.assertFalse(index.isEmpty());
        index.commit();
        index.close();


        // reopen
        index = getIndex(dir0, partitionName);
        Assert.assertFalse(index.isEmpty());


        index.merge(stream -> stream.stream(2L, UIO.longBytes(2), UIO.longBytes(2), System.currentTimeMillis(), false, Long.MAX_VALUE, 2L),
            null);
        index.getPointer(UIO.longBytes(2), UIO.longBytes(2), (prefix, key, timestamp, tombstoned, version, fp) -> {
            assertEquals(fp, 2);
            return true;
        });

        index.commit();
        index.close();

        // reopen
        index = getIndex(dir0, partitionName);
        Assert.assertFalse(index.isEmpty());

        index.merge((TxKeyPointerStream stream) -> {
            for (long i = 3; i < 100; i++) {
                if (!stream.stream(i, UIO.longBytes(i), UIO.longBytes(i), System.currentTimeMillis(), false, Long.MAX_VALUE, i)) {
                    return false;
                }
            }
            return true;
        }, null);

        testPutAsserts(index);

        index.commit();

        testPutAsserts(index);

    }

    private void testPutAsserts(LSMPointerIndexWALIndex index) throws Exception {
        // one at a time get
        for (long i = 0; i < 100; i++) {
            long expected = i;
            index.getPointer(UIO.longBytes(i), UIO.longBytes(i), (prefix, key, timestamp, tombstoned, version, fp) -> {
                assertEquals(fp, expected);
                return true;
            });
        }

        index.getPointers(UIO.longBytes(10), (UnprefixedWALKeyStream keyStream) -> {
            keyStream.stream(UIO.longBytes(10));
            return true;
        }, (prefix, key, timestamp, tombstoned, version, fp) -> {
            Assert.assertTrue(fp != -1);
            return true;
        });

        index.getPointers((stream) -> {
            for (long i = 0; i < 100; i++) {
                stream.stream(RowType.primary, UIO.longBytes(i), UIO.longBytes(i), null, 0, false, 0);
            }
            return true;
        }, (rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp) -> {

            System.out.println(
                "getPointers "
                    + " " + Long.toString(UIO.bytesLong(prefix), 2)
                    + " " + Long.toString(UIO.bytesLong(key), 2)
                    + " " + pointerTimestamp
                    + " " + pointerTombstoned
                    + " " + pointerVersion
                    + " " + pointerFp);

            Assert.assertTrue(pointerFp != -1);
            return true;
        });

        // contains
        index.containsKeys(UIO.longBytes(1000), (UnprefixedWALKeyStream keyStream) -> {
            keyStream.stream(UIO.longBytes(10000));
            keyStream.stream(UIO.longBytes(10001));
            keyStream.stream(UIO.longBytes(10002));
            return true;
        }, (byte[] prefix, byte[] key, boolean contained) -> {
            Assert.assertFalse(contained);
            return true;
        });

        index.containsKeys(UIO.longBytes(10), (UnprefixedWALKeyStream keyStream) -> {
            keyStream.stream(UIO.longBytes(10));
            return true;
        }, (byte[] prefix, byte[] key, boolean contained) -> {
            Assert.assertTrue(contained);
            return true;
        });

        int[] rowScanExpectedI = new int[] { 0 };
        index.rowScan((byte[] prefix, byte[] key, long timestamp, boolean tombstoned, long version, long fp) -> {
            System.out.println(
                "rowScan "
                    + " " + Long.toString(UIO.bytesLong(prefix), 2)
                    + " " + Long.toString(UIO.bytesLong(key), 2)
                    + " " + timestamp
                    + " " + tombstoned
                    + " " + version
                    + " " + fp);

            assertEquals(rowScanExpectedI[0], UIO.bytesLong(key));
            rowScanExpectedI[0]++;
            return true;
        });

        int[] rangeScaneExpectedI = new int[] { 10 };
        index.rangeScan(UIO.longBytes(10), UIO.longBytes(10), UIO.longBytes(20), UIO.longBytes(20),
            (byte[] prefix, byte[] key, long timestamp, boolean tombstoned, long version, long fp) -> {
                System.out.println(
                    "rangeScan "
                        + " " + Long.toString(UIO.bytesLong(prefix), 2)
                        + " " + Long.toString(UIO.bytesLong(key), 2)
                        + " " + timestamp
                        + " " + tombstoned
                        + " " + version
                        + " " + fp);

                Assert.assertTrue(rangeScaneExpectedI[0] < 20);
                assertEquals(rangeScaneExpectedI[0], UIO.bytesLong(key));
                rangeScaneExpectedI[0]++;
                return true;
            });
    }

    @Test
    public void testRangesNoPrefix() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()),
            VersionedPartitionName.STATIC_VERSION);
        LSMPointerIndexWALIndex index = getIndex(dir0, versionedPartitionName);

        index.merge((TxKeyPointerStream stream) -> {
            for (long i = 0; i < 64; i++) {
                if (!stream.stream(i, null, iToKey(i), System.currentTimeMillis(), false, Long.MAX_VALUE, i)) {
                    return false;
                }
            }
            return true;
        }, null);

        testRangeAsserts(index);

        index.commit();

        testRangeAsserts(index);
    }

    private void testRangeAsserts(LSMPointerIndexWALIndex index) throws Exception {
        int[] count = new int[1];
        byte[] fromKey = { 0, 1, 0, 0 };
        byte[] toKey = { 0, 2, 0, 0 };
        index.rangeScan(null, fromKey, null, toKey, (prefix, key, timestamp, tombstoned, version, fp) -> {
            count[0]++;
            System.out.println("prefix: " + Arrays.toString(prefix) + " key: " + Arrays.toString(key));
            Assert.assertTrue(UnsignedBytes.lexicographicalComparator().compare(fromKey, key) <= 0);
            Assert.assertTrue(UnsignedBytes.lexicographicalComparator().compare(key, toKey) < 0);
            return true;
        });
        Assert.assertEquals(count[0], 16);

        int[] rowScanExpectedI = new int[] { 0 };
        index.rowScan((byte[] prefix, byte[] key, long timestamp, boolean tombstoned, long version, long fp) -> {
            System.out.println(
                "rowScan "
                    + " " + Arrays.toString(prefix)
                    + " " + Arrays.toString(key)
                    + " " + timestamp
                    + " " + tombstoned
                    + " " + version
                    + " " + fp);

            //assertEquals(iToKey(rowScanExpectedI[0]), key);
            rowScanExpectedI[0]++;
            return true;
        });

        Assert.assertTrue(rowScanExpectedI[0] == 64);

        int[] rangeScaneExpectedI = new int[] { 8 };
        index.rangeScan(null, iToKey(8), null, iToKey(32),
            (byte[] prefix, byte[] key, long timestamp, boolean tombstoned, long version, long fp) -> {
                System.out.println(
                    "rangeScan "
                        + " " + Arrays.toString(prefix)
                        + " " + Arrays.toString(key)
                        + " " + timestamp
                        + " " + tombstoned
                        + " " + version
                        + " " + fp);

                Assert.assertTrue(rangeScaneExpectedI[0] < 32);
                assertEquals(iToKey(rangeScaneExpectedI[0]), key);
                rangeScaneExpectedI[0] += 4;
                return true;
            });
    }

    byte[] iToKey(long i) {
        return new byte[] { 0, (byte) (i % 4), (byte) (i % 2), (byte) i };
    }

    @Test
    public void testRangesPrefixed() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()),
            VersionedPartitionName.STATIC_VERSION);
        LSMPointerIndexWALIndex index = getIndex(dir0, versionedPartitionName);

        index.merge((TxKeyPointerStream stream) -> {
            for (long i = 0; i < 64; i++) {
                byte[] prefix = { 0, (byte) (i % 4) };
                byte[] key = { 0, 0, (byte) (i % 2), (byte) i };
                if (!stream.stream(i, prefix, key, System.currentTimeMillis(), false, Long.MAX_VALUE, i)) {
                    return false;
                }
            }
            return true;
        }, null);

        testRangesPrefixedAsserts(index);

        index.commit();

        testRangesPrefixedAsserts(index);

    }

    private void testRangesPrefixedAsserts(LSMPointerIndexWALIndex index) throws Exception {
        int[] count = new int[1];
        byte[] fromPrefix = { 0, 1 };
        byte[] toPrefix = { 0, 2 };
        index.rangeScan(fromPrefix, new byte[0], toPrefix, new byte[0], (prefix, key, timestamp, tombstoned, version, fp) -> {
            count[0]++;
            System.out.println("prefix: " + Arrays.toString(prefix) + " key: " + Arrays.toString(key));
            Assert.assertTrue(UnsignedBytes.lexicographicalComparator().compare(fromPrefix, prefix) <= 0);
            Assert.assertTrue(UnsignedBytes.lexicographicalComparator().compare(prefix, toPrefix) < 0);
            return true;
        });
        Assert.assertEquals(count[0], 16);
    }

    @Test
    public void testTakePrefixed() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()),
            VersionedPartitionName.STATIC_VERSION);
        LSMPointerIndexWALIndex index = getIndex(dir0, versionedPartitionName);

        index.merge((TxKeyPointerStream stream) -> {
            for (long i = 0; i < 64; i++) {
                byte[] prefix = { 0, (byte) (i % 4) };
                byte[] key = { 0, 0, (byte) (i % 2), (byte) i };
                if (!stream.stream(i, prefix, key, System.currentTimeMillis(), false, Long.MAX_VALUE, i)) {
                    return false;
                }
            }
            return true;
        }, null);

        testTakePrefixedAsserts(index);
        index.commit();
        testTakePrefixedAsserts(index);

    }

    private void testTakePrefixedAsserts(LSMPointerIndexWALIndex index) throws Exception {
        int[] count = new int[1];
        byte[] prefix = { 0, 1 };
        index.takePrefixUpdatesSince(prefix, 0, (txId, fp) -> {
            count[0]++;
            Assert.assertEquals(fp % 4, 1);
            return true;
        });
        Assert.assertEquals(count[0], 16);
    }

    @Test
    public void testCompact() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()),
            VersionedPartitionName.STATIC_VERSION);
        LSMPointerIndexWALIndex index = getIndex(dir0, versionedPartitionName);

        index.merge((TxKeyPointerStream stream) -> {
            for (long i = 0; i < 50; i++) {
                if (!stream.stream(i, UIO.longBytes(-i), UIO.longBytes(i), System.currentTimeMillis(), false, Long.MAX_VALUE, i)) {
                    return false;
                }
            }
            return true;
        }, null);

        for (long i = 0; i < 50; i++) {
            long expected = i;
            index.getPointer(UIO.longBytes(-i), UIO.longBytes(i), (prefix, key, timestamp, tombstoned, version, fp) -> {
                assertEquals(fp, expected);
                return true;
            });
        }

        CompactionWALIndex compactionWALIndex = index.startCompaction(true);
        compactionWALIndex.merge((stream) -> {
            for (long i = 100; i < 200; i++) {
                if (!stream.stream(i, UIO.longBytes(-i), UIO.longBytes(i), System.currentTimeMillis(), false, Long.MAX_VALUE, i)) {
                    return false;
                }
            }
            return true;
        });
        compactionWALIndex.commit(null);

        for (long i = 100; i < 200; i++) {
            long expected = i;
            index.getPointer(UIO.longBytes(-i), UIO.longBytes(i), (prefix, key, timestamp, tombstoned, version, fp) -> {
                System.out.println(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + fp);
                assertEquals(fp, expected);
                return true;
            });

        }
    }

    private LSMPointerIndexWALIndex getIndex(File dir, VersionedPartitionName partitionName) throws Exception {
        return new LSMPointerIndexWALIndexProvider("lsm", new PartitionStripeFunction(1), new File[] { dir })
            .createIndex(partitionName, 10_000);
    }

}
