package com.jivesoftware.os.amza.api.wal;

import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 *
 */
public class WALKeyTest {

    @Test
    public void testPrefixUpperExclusive() throws Exception {

        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            byte[] lower = new byte[] { 0, 0, 0, (byte) i };
            byte[] upper = WALKey.prefixUpperExclusive(lower);
            int c = KeyUtil.compare(lower, upper);
            Assert.assertTrue(c < 0, Arrays.toString(lower) + " vs " + Arrays.toString(upper) + " = " + c);
        }

    }
}