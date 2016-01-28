package com.jivesoftware.os.amza.service.collections;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class ConcurrentBAHashNGTest {

    @Test
    public void testPut() {

        ConcurrentBAHash<String> map = new ConcurrentBAHash<>(10, true, 2);
        for (byte i = 0; i < 16; i++) {
            map.put(new byte[]{i}, String.valueOf(i));
        }

        for (byte i = 0; i < 16; i++) {
            Assert.assertEquals(map.get(new byte[]{i}), String.valueOf(i));
        }
    }

}
