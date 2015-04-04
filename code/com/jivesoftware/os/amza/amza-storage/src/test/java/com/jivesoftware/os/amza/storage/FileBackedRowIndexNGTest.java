package com.jivesoftware.os.amza.storage;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.filer.io.FilerIO;
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
    public void testPut() {
        File dir0 = Files.createTempDir();
        File dir1 = Files.createTempDir();
        File dir2 = Files.createTempDir();
        RegionName regionName = new RegionName(false, "r1", "t1");

        FileBackedWALIndex index = new FileBackedWALIndex(regionName, 4, false, 0, new File[] { dir0, dir1, dir2 });
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(FilerIO.intBytes(1)), new WALPointer(1L, System.currentTimeMillis(), false))));

        WALPointer got = index.getPointers(Collections.singletonList(new WALKey(FilerIO.intBytes(1)))).get(0);
        Assert.assertEquals(got.getFp(), 1L);

        // reopen
        index = new FileBackedWALIndex(regionName, 4, false, 0, new File[] { dir0, dir1, dir2 });
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(FilerIO.intBytes(2)), new WALPointer(2L, System.currentTimeMillis(), false))));
        got = index.getPointers(Collections.singletonList(new WALKey(FilerIO.intBytes(2)))).get(0);
        Assert.assertEquals(got.getFp(), 2L);

        for (int i = 0; i < 100; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(FilerIO.intBytes(i)), new WALPointer((long) i, System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 100; i++) {
            got = index.getPointers(Collections.singletonList(new WALKey(FilerIO.intBytes(i)))).get(0);
            Assert.assertEquals(got.getFp(), (long) i);
        }
    }

    @Test
    public void testCompact() throws Exception {

        File dir0 = Files.createTempDir();
        File dir1 = Files.createTempDir();
        File dir2 = Files.createTempDir();
        RegionName regionName = new RegionName(false, "r1", "t1");
        FileBackedWALIndex index = new FileBackedWALIndex(regionName, 4, false, 0, new File[] { dir0, dir1, dir2 });

        for (int i = 0; i < 50; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(FilerIO.intBytes(i)),
                new WALPointer((long) i, System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 50; i++) {
            WALPointer got = index.getPointers(Collections.singletonList(new WALKey(FilerIO.intBytes(i)))).get(0);
            Assert.assertEquals(got.getFp(), (long) i);
        }

        WALIndex.CompactionWALIndex startCompaction = index.startCompaction();
        for (int i = 100; i < 200; i++) {
            startCompaction.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(FilerIO.intBytes(i)),
                new WALPointer((long) i, System.currentTimeMillis(), false))));
        }
        startCompaction.commit();

        for (int i = 100; i < 200; i++) {
            WALPointer got = index.getPointers(Collections.singletonList(new WALKey(FilerIO.intBytes(i)))).get(0);
            Assert.assertEquals(got.getFp(), (long) i);
        }
    }

}
