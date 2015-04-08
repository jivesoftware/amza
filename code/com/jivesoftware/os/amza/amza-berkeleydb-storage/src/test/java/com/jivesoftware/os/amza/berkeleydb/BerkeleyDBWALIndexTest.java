package com.jivesoftware.os.amza.berkeleydb;

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
 *
 */
public class BerkeleyDBWALIndexTest {

    @Test
    public void testPut() throws Exception {
        File dir0 = Files.createTempDir();
        RegionName regionName = new RegionName(false, "r1", "t1");
        BerkeleyDBWALIndex index = getIndex(dir0, regionName);
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(FilerIO.intBytes(1)), new WALPointer(1L, System.currentTimeMillis(), false))));

        WALPointer got = index.getPointer(new WALKey(FilerIO.intBytes(1)));
        Assert.assertEquals(got.getFp(), 1);
        index.close();

        // reopen
        index = getIndex(dir0, regionName);
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(FilerIO.intBytes(2)), new WALPointer(2L, System.currentTimeMillis(), false))));
        got = index.getPointer(new WALKey(FilerIO.intBytes(2)));
        Assert.assertEquals(got.getFp(), 2);

        for (int i = 0; i < 100; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(FilerIO.intBytes(i)), new WALPointer((long) i, System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 100; i++) {
            got = index.getPointer(new WALKey(FilerIO.intBytes(i)));
            Assert.assertEquals(got.getFp(), i);
        }
    }

    @Test
    public void testCompact() throws Exception {

        File dir0 = Files.createTempDir();
        RegionName regionName = new RegionName(false, "r1", "t1");
        BerkeleyDBWALIndex index = getIndex(dir0, regionName);

        for (int i = 0; i < 50; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(FilerIO.intBytes(i)),
                new WALPointer((long) i, System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 50; i++) {
            WALPointer got = index.getPointer(new WALKey(FilerIO.intBytes(i)));
            Assert.assertEquals(got.getFp(), i);
        }

        WALIndex.CompactionWALIndex startCompaction = index.startCompaction();
        for (int i = 100; i < 200; i++) {
            startCompaction.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new WALKey(FilerIO.intBytes(i)),
                new WALPointer((long) i, System.currentTimeMillis(), false))));
        }
        startCompaction.commit();

        for (int i = 100; i < 200; i++) {
            WALPointer got = index.getPointer(new WALKey(FilerIO.intBytes(i)));
            Assert.assertEquals(got.getFp(), (long) i);
        }
    }

    private BerkeleyDBWALIndex getIndex(File dir0, RegionName regionName) throws Exception {
        return new BerkeleyDBWALIndexProvider(new String[] { dir0.getAbsolutePath() }, 1).createIndex(regionName);
    }

}