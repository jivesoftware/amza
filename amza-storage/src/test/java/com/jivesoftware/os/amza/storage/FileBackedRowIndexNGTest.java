package com.jivesoftware.os.amza.storage;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.File;
import java.util.AbstractMap;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class FileBackedRowIndexNGTest {

    @Test
    public void testPut() throws Exception {
        File dir0 = Files.createTempDir();
        File dir1 = Files.createTempDir();
        File dir2 = Files.createTempDir();
        PartitionName partitionName = new PartitionName(false, "r1", "t1");

        FileBackedWALIndex index = new FileBackedWALIndex(partitionName, 4, false, 0, new File[]{dir0, dir1, dir2});
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(UIO.intBytes(1)), new WALPointer(1L, System.currentTimeMillis(), false))));

        WALPointer got = index.getPointer(new WALKey(UIO.intBytes(1)));
        Assert.assertEquals(got.getFp(), 1L);

        // reopen
        index = new FileBackedWALIndex(partitionName, 4, false, 0, new File[]{dir0, dir1, dir2});
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(UIO.intBytes(2)), new WALPointer(2L, System.currentTimeMillis(), false))));
        got = index.getPointer(new WALKey(UIO.intBytes(2)));
        Assert.assertEquals(got.getFp(), 2L);

        for (int i = 0; i < 100; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(UIO.intBytes(i)), new WALPointer((long) i, System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 100; i++) {
            got = index.getPointer(new WALKey(UIO.intBytes(i)));
            Assert.assertEquals(got.getFp(), (long) i);
        }
    }

    @Test
    public void testCompact() throws Exception {

        File dir0 = Files.createTempDir();
        File dir1 = Files.createTempDir();
        File dir2 = Files.createTempDir();
        PartitionName partitionName = new PartitionName(false, "r1", "t1");
        FileBackedWALIndex index = new FileBackedWALIndex(partitionName, 4, false, 0, new File[]{dir0, dir1, dir2});

        for (int i = 0; i < 50; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(UIO.intBytes(i)),
                new WALPointer((long) i, System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 50; i++) {
            WALPointer got = index.getPointer(new WALKey(UIO.intBytes(i)));
            Assert.assertEquals(got.getFp(), (long) i);
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
            Assert.assertEquals(got.getFp(), (long) i);
        }
    }

}
