/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.mavibot;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowsIndex;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class MavibotRowIndexNGTest {

    @Test
    public void testPut() throws IOException, BTreeAlreadyManagedException {
        File dir0 = Files.createTempDir();
        TableName table = new TableName("r1", "t1", null, null);
        MavibotRowIndex index = new MavibotRowIndex(dir0, table);
        index.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
            new RowIndexKey(FilerIO.intBytes(1)), new RowIndexValue(FilerIO.longBytes(1), System.currentTimeMillis(), false))));

        RowIndexValue got = index.get(Collections.singletonList(new RowIndexKey(FilerIO.intBytes(1)))).get(0);
        Assert.assertEquals(FilerIO.bytesLong(got.getValue()), 1);
        index.close();

        // reopen
        index = new MavibotRowIndex(dir0, table);
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
        TableName table = new TableName("r1", "t1", null, null);
        MavibotRowIndex index = new MavibotRowIndex(dir0, table);

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
