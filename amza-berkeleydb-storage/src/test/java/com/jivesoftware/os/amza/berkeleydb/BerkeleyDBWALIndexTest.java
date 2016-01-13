package com.jivesoftware.os.amza.berkeleydb;

import com.google.common.io.Files;
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.CompactionWALIndex;
import com.jivesoftware.os.amza.api.stream.TxKeyPointerStream;
import java.io.File;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class BerkeleyDBWALIndexTest {

    @Test
    public void testPut() throws Exception {
        File dir0 = Files.createTempDir();
        VersionedPartitionName partitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()),
            VersionedPartitionName.STATIC_VERSION);
        BerkeleyDBWALIndex index = getIndex(dir0, partitionName);
        index.merge(stream -> stream.stream(1L, UIO.longBytes(-1), UIO.longBytes(1), System.currentTimeMillis(), false, Long.MAX_VALUE, 1L),
            null);

        index.getPointer(UIO.longBytes(-1), UIO.longBytes(1), (prefix, key, timestamp, tombstoned, version, fp) -> {
            assertEquals(fp, 1);
            return true;
        });

        index.close();

        // reopen
        index = getIndex(dir0, partitionName);
        index.merge(stream -> stream.stream(2L, UIO.longBytes(-2), UIO.longBytes(2), System.currentTimeMillis(), false, Long.MAX_VALUE, 2L),
            null);
        index.getPointer(UIO.longBytes(-2), UIO.longBytes(2), (prefix, key, timestamp, tombstoned, version, fp) -> {
            assertEquals(fp, 2);
            return true;
        });

        index.merge((TxKeyPointerStream stream) -> {
            for (long i = 0; i < 100; i++) {
                if (!stream.stream(i, UIO.longBytes(-i), UIO.longBytes(i), System.currentTimeMillis(), false, Long.MAX_VALUE, i)) {
                    return false;
                }
            }
            return true;
        }, null);

        for (long i = 0; i < 100; i++) {
            long expected = i;
            index.getPointer(UIO.longBytes(-i), UIO.longBytes(i), (prefix, key, timestamp, tombstoned, version, fp) -> {
                assertEquals(fp, expected);
                return true;
            });

        }
    }

    @Test
    public void testRangesNoPrefix() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()),
            VersionedPartitionName.STATIC_VERSION);
        BerkeleyDBWALIndex index = getIndex(dir0, versionedPartitionName);

        index.merge((TxKeyPointerStream stream) -> {
            for (long i = 0; i < 64; i++) {
                byte[] key = { 0, (byte) (i % 4), (byte) (i % 2), (byte) i };
                if (!stream.stream(i, null, key, System.currentTimeMillis(), false, Long.MAX_VALUE, i)) {
                    return false;
                }
            }
            return true;
        }, null);

        int[] count = new int[1];
        byte[] fromKey = { 0, 1, 0, 0 };
        byte[] toKey = { 0, 2, 0, 0 };
        index.rangeScan(null, fromKey, null, toKey, (prefix, key, timestamp, tombstoned, version, fp) -> {
            count[0]++;
            Assert.assertTrue(UnsignedBytes.lexicographicalComparator().compare(fromKey, key) <= 0);
            Assert.assertTrue(UnsignedBytes.lexicographicalComparator().compare(key, toKey) < 0);
            //System.out.println("prefix: " + Arrays.toString(prefix) + " key: " + Arrays.toString(key));
            return true;
        });
        Assert.assertEquals(count[0], 16);
    }

    @Test
    public void testRangesPrefixed() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()),
            VersionedPartitionName.STATIC_VERSION);
        BerkeleyDBWALIndex index = getIndex(dir0, versionedPartitionName);

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

        int[] count = new int[1];
        byte[] fromPrefix = { 0, 1 };
        byte[] toPrefix = { 0, 2 };
        index.rangeScan(fromPrefix, new byte[0], toPrefix, new byte[0], (prefix, key, timestamp, tombstoned, version, fp) -> {
            count[0]++;
            Assert.assertTrue(UnsignedBytes.lexicographicalComparator().compare(fromPrefix, prefix) <= 0);
            Assert.assertTrue(UnsignedBytes.lexicographicalComparator().compare(prefix, toPrefix) < 0);
            //System.out.println("prefix: " + Arrays.toString(prefix) + " key: " + Arrays.toString(key));
            return true;
        });
        Assert.assertEquals(count[0], 16);
    }

    @Test
    public void testTakePrefixed() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()),
            VersionedPartitionName.STATIC_VERSION);
        BerkeleyDBWALIndex index = getIndex(dir0, versionedPartitionName);

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
        BerkeleyDBWALIndex index = getIndex(dir0, versionedPartitionName);

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
        compactionWALIndex.merge((TxKeyPointerStream stream) -> {
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

    private BerkeleyDBWALIndex getIndex(File dir, VersionedPartitionName partitionName) throws Exception {
        return new BerkeleyDBWALIndexProvider("berkeleydb", new PartitionStripeFunction(1), new String[] { dir.getAbsolutePath() })
            .createIndex(partitionName, 1000);
    }

}
