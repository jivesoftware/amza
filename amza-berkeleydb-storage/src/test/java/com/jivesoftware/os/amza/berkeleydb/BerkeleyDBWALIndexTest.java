package com.jivesoftware.os.amza.berkeleydb;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import java.io.File;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class BerkeleyDBWALIndexTest {

    @Test
    public void testPut() throws Exception {
        File dir0 = Files.createTempDir();
        VersionedPartitionName partitionName = new VersionedPartitionName(new PartitionName(false, "r1", "t1"), 0);
        BerkeleyDBWALIndex index = getIndex(dir0, partitionName);
        index.merge((WALKeyPointerStream stream) -> {
            stream.stream(new WALKey(UIO.intBytes(1)), System.currentTimeMillis(), false, 1L);
        }, null);

        index.getPointer(new WALKey(UIO.intBytes(1)), (WALKey key, long timestamp, boolean tombstoned, long fp) -> {
            assertEquals(fp, 1);
            return true;
        });

        index.close();

        // reopen
        index = getIndex(dir0, partitionName);
        index.merge((WALKeyPointerStream stream) -> {
            stream.stream(new WALKey(UIO.intBytes(2)), System.currentTimeMillis(), false, 2L);
        }, null);
        index.getPointer(new WALKey(UIO.intBytes(2)), (WALKey key, long timestamp, boolean tombstoned, long fp) -> {
            assertEquals(fp, 2);
            return true;
        });

        index.merge((WALKeyPointerStream stream) -> {
            for (int i = 0; i < 100; i++) {
                stream.stream(new WALKey(UIO.intBytes(i)), System.currentTimeMillis(), false, i);
            }
        }, null);

        for (int i = 0; i < 100; i++) {
            int expected = i;
            index.getPointer(new WALKey(UIO.intBytes(i)), (WALKey key, long timestamp, boolean tombstoned, long fp) -> {
                assertEquals(fp, expected);
                return true;
            });

        }
    }

    @Test
    public void testCompact() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1", "t1"), 0);
        BerkeleyDBWALIndex index = getIndex(dir0, versionedPartitionName);

        index.merge((WALKeyPointerStream stream) -> {
            for (int i = 0; i < 50; i++) {
                stream.stream(new WALKey(UIO.intBytes(i)), System.currentTimeMillis(), false, i);
            }
        }, null);

        for (int i = 0; i < 50; i++) {
            int expected = i;
            index.getPointer(new WALKey(UIO.intBytes(i)), (WALKey key, long timestamp, boolean tombstoned, long fp) -> {
                assertEquals(fp, expected);
                return true;
            });
        }

        WALIndex.CompactionWALIndex compactionWALIndex = index.startCompaction();
        compactionWALIndex.merge((WALKeyPointerStream stream) -> {
            for (int i = 100; i < 200; i++) {
                stream.stream(new WALKey(UIO.intBytes(i)), System.currentTimeMillis(), false, i);
            }
        });
        compactionWALIndex.commit();

        for (int i = 100; i < 200; i++) {
            int expected = i;
            index.getPointer(new WALKey(UIO.intBytes(i)), (WALKey key, long timestamp, boolean tombstoned, long fp) -> {
                System.out.println(key + " " + timestamp + " " + tombstoned + " " + fp);
                //assertEquals(fp, (long) expected);
                return true;
            });

        }
    }

    private BerkeleyDBWALIndex getIndex(File dir0, VersionedPartitionName partitionName) throws Exception {
        return new BerkeleyDBWALIndexProvider(new String[]{dir0.getAbsolutePath()}, 1).createIndex(partitionName);
    }

}
