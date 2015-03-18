/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.storage.binary;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RowReader;
import com.jivesoftware.os.amza.storage.filer.Filer;
import com.jivesoftware.os.amza.storage.filer.IFiler;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan
 */
public class BinaryRowReaderWriterTest {

    /**
     * Test of read method, of class BinaryRowReader.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testRead() throws Exception {
        File dir = Files.createTempDir();

        IFiler filer = new Filer(new File(dir, "booya").getAbsolutePath(), "rw");
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer);

        ReadStream readStream = new ReadStream();
        binaryRowReader.reverseScan(readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowReader.scan(0, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowWriter.write(Collections.singletonList(new byte[]{1, 2, 3, 4}), false);
        binaryRowReader.scan(0, readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowReader.reverseScan(readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowWriter.write(Collections.singletonList(new byte[]{2, 3, 4, 5}), true);
        binaryRowReader.scan(0, readStream);
        Assert.assertEquals(readStream.rows.size(), 2);
        readStream.clear();

        binaryRowReader.reverseScan(readStream);
        Assert.assertEquals(readStream.rows.size(), 2);
        Assert.assertTrue(Arrays.equals(readStream.rows.get(0), new byte[]{2, 3, 4, 5}));
        Assert.assertTrue(Arrays.equals(readStream.rows.get(1), new byte[]{1, 2, 3, 4}));
        readStream.clear();

    }

    @Test
    public void testOpenCloseAppend() throws Exception {
        File dir = Files.createTempDir();

        Random rand = new Random();
        for (int i = 0; i < 1000; i++) {
            IFiler filer = new Filer(new File(dir, "foo").getAbsolutePath(), "rw");
            BinaryRowReader binaryRowReader = new BinaryRowReader(filer);
            BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer);

            ReadStream readStream = new ReadStream();

            if (i > 0) {
                binaryRowReader.reverseScan(readStream);
                Assert.assertEquals(readStream.rows.size(), i);
            }
            readStream.clear();

            byte[] row = new byte[4];
            rand.nextBytes(row);
            binaryRowWriter.write(Arrays.asList(row), true);
            filer.close();
        }

    }

    static class ReadStream implements RowReader.Stream<byte[]> {

        int clears = 0;
        private final ArrayList<byte[]> rows = new ArrayList<>();

        @Override
        public boolean row(long rowPointer, byte[] row) throws Exception {
            rows.add(row);
            return true;
        }

        void clear() {
            clears++;
            rows.clear();
        }
    }

}
