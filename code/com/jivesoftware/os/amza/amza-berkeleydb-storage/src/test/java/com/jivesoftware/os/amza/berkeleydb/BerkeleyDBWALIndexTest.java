package com.jivesoftware.os.amza.berkeleydb;

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
 */
public class BerkeleyDBWALIndexTest {

    @Test
    public void testPut() throws Exception {
        File dir0 = Files.createTempDir();
        RegionName regionName = new RegionName(false, "r1", "t1");
        BerkeleyDBWALIndex index = new BerkeleyDBWALIndex(dir0, regionName);
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new WALKey(FilerIO.intBytes(1)), new WALValue(FilerIO.longBytes(1), System.currentTimeMillis(), false))));

        WALValue got = index.get(Collections.singletonList(new WALKey(FilerIO.intBytes(1)))).get(0);
        Assert.assertEquals(FilerIO.bytesLong(got.getValue()), 1);
        index.close();

        // reopen
        index = new BerkeleyDBWALIndex(dir0, regionName);
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
        RegionName regionName = new RegionName(false, "r1", "t1");
        BerkeleyDBWALIndex index = new BerkeleyDBWALIndex(dir0, regionName);

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