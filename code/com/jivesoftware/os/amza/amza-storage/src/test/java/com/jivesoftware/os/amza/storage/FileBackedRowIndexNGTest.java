package com.jivesoftware.os.amza.storage;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowsIndex;
import com.jivesoftware.os.amza.shared.TableName;
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
        TableName table = new TableName("r1", "t1", null, null);

        FileBackedRowIndex index = new FileBackedRowIndex(table, 4, false, 0, new File[]{dir0, dir1, dir2});
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new RowIndexKey(FilerIO.intBytes(1)), new RowIndexValue(FilerIO.longBytes(1), System.currentTimeMillis(), false))));

        RowIndexValue got = index.get(Collections.singletonList(new RowIndexKey(FilerIO.intBytes(1)))).get(0);
        Assert.assertEquals(FilerIO.bytesLong(got.getValue()), 1);

        // reopen
        index = new FileBackedRowIndex(table, 4, false, 0, new File[]{dir0, dir1, dir2});
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new RowIndexKey(FilerIO.intBytes(2)), new RowIndexValue(FilerIO.longBytes(2), System.currentTimeMillis(), false))));
        got = index.get(Collections.singletonList(new RowIndexKey(FilerIO.intBytes(2)))).get(0);
        Assert.assertEquals(FilerIO.bytesLong(got.getValue()), 2);

        for (int i = 0; i < 100; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new RowIndexKey(FilerIO.intBytes(i)), new RowIndexValue(FilerIO.longBytes(i), System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 100; i++) {
            got = index.get(Collections.singletonList(new RowIndexKey(FilerIO.intBytes(i)))).get(0);
            Assert.assertEquals(FilerIO.bytesLong(got.getValue()), i);
        }
    }

    @Test
    public void testCompact() throws Exception {

        File dir0 = Files.createTempDir();
        File dir1 = Files.createTempDir();
        File dir2 = Files.createTempDir();
        TableName table = new TableName("r1", "t1", null, null);
        FileBackedRowIndex index = new FileBackedRowIndex(table, 4, false, 0, new File[]{dir0, dir1, dir2});

        for (int i = 0; i < 50; i++) {
            index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new RowIndexKey(FilerIO.intBytes(i)),
                new RowIndexValue(FilerIO.longBytes(i), System.currentTimeMillis(), false))));
        }

        for (int i = 0; i < 50; i++) {
            RowIndexValue got = index.get(Collections.singletonList(new RowIndexKey(FilerIO.intBytes(i)))).get(0);
            Assert.assertEquals(FilerIO.bytesLong(got.getValue()), i);
        }

        RowsIndex.CompactionRowIndex startCompaction = index.startCompaction();
        for (int i = 100; i < 200; i++) {
            startCompaction.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                new RowIndexKey(FilerIO.intBytes(i)),
                new RowIndexValue(FilerIO.longBytes(i), System.currentTimeMillis(), false))));
        }
        startCompaction.commit();

        for (int i = 100; i < 200; i++) {
            RowIndexValue got = index.get(Collections.singletonList(new RowIndexKey(FilerIO.intBytes(i)))).get(0);
            Assert.assertEquals(FilerIO.bytesLong(got.getValue()), i);
        }
    }

}
