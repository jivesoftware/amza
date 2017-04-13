package com.jivesoftware.os.amza.api;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class BAInternerTest {

    @Test
    public void testIntern() throws InterruptedException {
        BAInterner interner = new BAInterner();
        byte[] sequence = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 };

        int[] codes = new int[10];
        for (int i = 0; i < 10; i++) {
            byte[] interned = interner.intern(sequence, i, 10);
            codes[i] = System.identityHashCode(interned);
        }

        for (int i = 0; i < 10; i++) {
            byte[] interned = interner.intern(sequence, i, 10);
            Assert.assertEquals(System.identityHashCode(interned), codes[i]);
        }
    }

}