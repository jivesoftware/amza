package com.jivesoftware.os.amza.storage;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.File;
import java.util.AbstractMap;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class FileBackedRowIndexNGTest {

    @Test
    public void testPut() {
        File dir0 = Files.createTempDir();
        File dir1 = Files.createTempDir();
        File dir2 = Files.createTempDir();
        RegionName regionName = new RegionName("r1", "t1", null, null);

        FileBackedWALIndex index = new FileBackedWALIndex(regionName, 4, false, 0, new File[]{dir0, dir1, dir2});
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(FilerIO.intBytes(1)), new WALValue(FilerIO.longBytes(1), System.currentTimeMillis(), false))));

        WALValue got = index.get(Collections.singletonList(new WALKey(FilerIO.intBytes(1)))).get(0);
        Assert.assertEquals(FilerIO.bytesLong(got.getValue()), 1);

        // reopen
        index = new FileBackedWALIndex(regionName, 4, false, 0, new File[]{dir0, dir1, dir2});
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(FilerIO.intBytes(2)), new WALValue(FilerIO.longBytes(2), System.currentTimeMillis(), false))));
        got = index.get(Collections.singletonList(new WALKey(FilerIO.intBytes(2)))).get(0);
        Assert.assertEquals(FilerIO.bytesLong(got.getValue()), 2);

        for (int i = 0; i < 100; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(FilerIO.intBytes(i)), new WALValue(FilerIO.longBytes(i), System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 100; i++) {
            got = index.get(Collections.singletonList(new WALKey(FilerIO.intBytes(i)))).get(0);
            Assert.assertEquals(FilerIO.bytesLong(got.getValue()), i);
        }
    }

    @Test
    public void testCompact() throws Exception {

        File dir0 = Files.createTempDir();
        File dir1 = Files.createTempDir();
        File dir2 = Files.createTempDir();
        RegionName regionName = new RegionName("r1", "t1", null, null);
        FileBackedWALIndex index = new FileBackedWALIndex(regionName, 4, false, 0, new File[]{dir0, dir1, dir2});

        for (int i = 0; i < 50; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(FilerIO.intBytes(i)),
                new WALValue(FilerIO.longBytes(i), System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 50; i++) {
            WALValue got = index.get(Collections.singletonList(new WALKey(FilerIO.intBytes(i)))).get(0);
            Assert.assertEquals(FilerIO.bytesLong(got.getValue()), i);
        }

        WALIndex.CompactionWALIndex startCompaction = index.startCompaction();
        for (int i = 100; i < 200; i++) {
            startCompaction.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(FilerIO.intBytes(i)),
                new WALValue(FilerIO.longBytes(i), System.currentTimeMillis(), false))));
        }
        startCompaction.commit();

        for (int i = 100; i < 200; i++) {
            WALValue got = index.get(Collections.singletonList(new WALKey(FilerIO.intBytes(i)))).get(0);
            Assert.assertEquals(FilerIO.bytesLong(got.getValue()), i);
        }
    }

}
