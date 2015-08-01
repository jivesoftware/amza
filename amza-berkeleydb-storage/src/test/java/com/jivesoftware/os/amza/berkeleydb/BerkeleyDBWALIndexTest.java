package com.jivesoftware.os.amza.berkeleydb;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import java.io.File;
import java.util.Arrays;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class BerkeleyDBWALIndexTest {

    @Test
    public void testPut() throws Exception {
        File dir0 = Files.createTempDir();
        VersionedPartitionName partitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()), 0);
        BerkeleyDBWALIndex index = getIndex(dir0, partitionName);
        index.merge(stream -> stream.stream(1L, UIO.longBytes(-1), UIO.longBytes(1), System.currentTimeMillis(), false, 1L),
            null);

        index.getPointer(UIO.longBytes(-1), UIO.longBytes(1), (prefix, key, timestamp, tombstoned, fp) -> {
            assertEquals(fp, 1);
            return true;
        });

        index.close();

        // reopen
        index = getIndex(dir0, partitionName);
        index.merge(stream -> stream.stream(2L, UIO.longBytes(-2), UIO.longBytes(2), System.currentTimeMillis(), false, 2L),
            null);
        index.getPointer(UIO.longBytes(-2), UIO.longBytes(2), (prefix, key, timestamp, tombstoned, fp) -> {
            assertEquals(fp, 2);
            return true;
        });

        index.merge((TxKeyPointerStream stream) -> {
            for (long i = 0; i < 100; i++) {
                if (!stream.stream(i, UIO.longBytes(-i), UIO.longBytes(i), System.currentTimeMillis(), false, i)) {
                    return false;
                }
            }
            return true;
        }, null);

        for (long i = 0; i < 100; i++) {
            long expected = i;
            index.getPointer(UIO.longBytes(-i), UIO.longBytes(i), (prefix, key, timestamp, tombstoned, fp) -> {
                assertEquals(fp, expected);
                return true;
            });

        }
    }

    @Test
    public void testCompact() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1".getBytes(), "t1".getBytes()), 0);
        BerkeleyDBWALIndex index = getIndex(dir0, versionedPartitionName);

        index.merge((TxKeyPointerStream stream) -> {
            for (long i = 0; i < 50; i++) {
                if (!stream.stream(i, UIO.longBytes(-i), UIO.longBytes(i), System.currentTimeMillis(), false, i)) {
                    return false;
                }
            }
            return true;
        }, null);

        for (long i = 0; i < 50; i++) {
            long expected = i;
            index.getPointer(UIO.longBytes(-i), UIO.longBytes(i), (prefix, key, timestamp, tombstoned, fp) -> {
                assertEquals(fp, expected);
                return true;
            });
        }

        WALIndex.CompactionWALIndex compactionWALIndex = index.startCompaction();
        compactionWALIndex.merge((TxKeyPointerStream stream) -> {
            for (long i = 100; i < 200; i++) {
                if (!stream.stream(i, UIO.longBytes(-i), UIO.longBytes(i), System.currentTimeMillis(), false, i)) {
                    return false;
                }
            }
            return true;
        });
        compactionWALIndex.commit();

        for (long i = 100; i < 200; i++) {
            long expected = i;
            index.getPointer(UIO.longBytes(-i), UIO.longBytes(i), (prefix, key, timestamp, tombstoned, fp) -> {
                System.out.println(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + fp);
                assertEquals(fp, expected);
                return true;
            });

        }
    }

    private BerkeleyDBWALIndex getIndex(File dir0, VersionedPartitionName partitionName) throws Exception {
        return new BerkeleyDBWALIndexProvider(new String[] { dir0.getAbsolutePath() }, 1).createIndex(partitionName);
    }

}
