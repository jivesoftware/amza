/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.TableRowReader;
import com.jivesoftware.os.amza.storage.chunks.Filer;
import com.jivesoftware.os.amza.storage.chunks.IFiler;
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

        IFiler filer = Filer.open("booya", "rw");
        BinaryRowReader binaryRowReader = new BinaryRowReader(filer);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(filer);

        ReadStream readStream = new ReadStream();
        binaryRowReader.read(true, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowReader.read(false, readStream);
        Assert.assertTrue(readStream.rows.isEmpty());
        readStream.clear();

        binaryRowWriter.write(Arrays.asList(new byte[]{1, 2, 3, 4}), false);
        binaryRowReader.read(false, readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowReader.read(true, readStream);
        Assert.assertEquals(readStream.rows.size(), 1);
        readStream.clear();

        binaryRowWriter.write(Arrays.asList(new byte[]{2, 3, 4, 5}), true);
        binaryRowReader.read(false, readStream);
        Assert.assertEquals(readStream.rows.size(), 2);
        readStream.clear();

        binaryRowReader.read(true, readStream);
        Assert.assertEquals(readStream.rows.size(), 2);
        Assert.assertTrue(Arrays.equals(readStream.rows.get(0), new byte[]{2, 3, 4, 5}));
        Assert.assertTrue(Arrays.equals(readStream.rows.get(1), new byte[]{1, 2, 3, 4}));
        readStream.clear();

    }

    static class ReadStream implements TableRowReader.Stream<byte[]> {

        int clears = 0;
        private final ArrayList<byte[]> rows = new ArrayList<>();

        @Override
        public boolean stream(byte[] row) throws Exception {
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
