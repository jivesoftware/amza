package com.jivesoftware.os.amza.berkeleydb;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.File;
import java.util.AbstractMap;
import java.util.Collections;
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
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(UIO.intBytes(1)), new WALPointer(1L, System.currentTimeMillis(), false))));

        WALPointer got = index.getPointer(new WALKey(UIO.intBytes(1)));
        assertEquals(got.getFp(), 1);
        index.close();

        // reopen
        index = getIndex(dir0, partitionName);
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(UIO.intBytes(2)), new WALPointer(2L, System.currentTimeMillis(), false))));
        got = index.getPointer(new WALKey(UIO.intBytes(2)));
        assertEquals(got.getFp(), 2);

        for (int i = 0; i < 100; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(UIO.intBytes(i)), new WALPointer((long) i, System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 100; i++) {
            got = index.getPointer(new WALKey(UIO.intBytes(i)));
            assertEquals(got.getFp(), i);
        }
    }

    @Test
    public void testCompact() throws Exception {

        File dir0 = Files.createTempDir();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "r1", "t1"), 0);
        BerkeleyDBWALIndex index = getIndex(dir0, versionedPartitionName);

        for (int i = 0; i < 50; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(UIO.intBytes(i)),
                new WALPointer((long) i, System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 50; i++) {
            WALPointer got = index.getPointer(new WALKey(UIO.intBytes(i)));
            assertEquals(got.getFp(), i);
        }

        WALIndex.CompactionWALIndex startCompaction = index.startCompaction();
        for (int i = 100; i < 200; i++) {
            startCompaction.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(UIO.intBytes(i)),
                new WALPointer((long) i, System.currentTimeMillis(), false))));
        }
        startCompaction.commit();

        for (int i = 100; i < 200; i++) {
            WALPointer got = index.getPointer(new WALKey(UIO.intBytes(i)));
            assertEquals(got.getFp(), (long) i);
        }
    }

    private BerkeleyDBWALIndex getIndex(File dir0, VersionedPartitionName partitionName) throws Exception {
        return new BerkeleyDBWALIndexProvider(new String[]{dir0.getAbsolutePath()}, 1).createIndex(partitionName);
    }

}
