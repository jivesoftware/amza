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

import com.jivesoftware.os.amza.shared.RowReader;
import com.jivesoftware.os.amza.storage.filer.Filer;
import com.jivesoftware.os.amza.storage.filer.IFiler;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan
 */
public class BinaryRowReaderWriterTest {

    @AfterClass
    public void cleanup() {
        new File("booya").deleteOnExit();
    }
    /**
     * Test of read method, of class BinaryRowReader.
     */
    @Test
    public void testRead() throws Exception {

        IFiler filer = new Filer("booya", "rw");
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer);

        ReadStream readStream = new ReadStream();
        binaryRowReader.scan(true, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowReader.scan(false, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowWriter.write(Arrays.asList(new byte[]{1, 2, 3, 4}), false);
        binaryRowReader.scan(false, readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowReader.scan(true, readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowWriter.write(Arrays.asList(new byte[]{2, 3, 4, 5}), true);
        binaryRowReader.scan(false, readStream);
        Assert.assertEquals(readStream.rows.size(), 2);
        readStream.clear();

        binaryRowReader.scan(true, readStream);
        Assert.assertEquals(readStream.rows.size(), 2);
        Assert.assertTrue(Arrays.equals(readStream.rows.get(0), new byte[]{2, 3, 4, 5}));
        Assert.assertTrue(Arrays.equals(readStream.rows.get(1), new byte[]{1, 2, 3, 4}));
        readStream.clear();

    }

    static class ReadStream implements RowReader.Stream<byte[]> {

        int clears = 0;
        private final ArrayList<byte[]> rows = new ArrayList<>();

        @Override
        public boolean row(byte[] rowPointer, byte[] row) throws Exception {
            rows.add(row);
            System.out.println(clears + " row:" + Arrays.toString(row));
            return true;
        }

        void clear() {
            clears++;
            rows.clear();
        }
    }

}
