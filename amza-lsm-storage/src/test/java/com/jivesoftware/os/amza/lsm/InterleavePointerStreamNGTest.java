package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.RawNextPointer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import org.junit.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class InterleavePointerStreamNGTest {

    @Test
    public void testNext() throws Exception {

        InterleavePointerStream ips = new InterleavePointerStream(new RawNextPointer[]{
            nextPointerSequence(new long[]{1, 2, 3, 4, 5}, new long[]{3, 3, 3, 3, 3})
        });

        List<Expected> expected = new ArrayList<>();
        expected.add(new Expected(1, 3));
        expected.add(new Expected(2, 3));
        expected.add(new Expected(3, 3));
        expected.add(new Expected(4, 3));
        expected.add(new Expected(5, 3));

        assertExpected(ips, expected);
    }

    private void assertExpected(InterleavePointerStream ips, List<Expected> expected) throws Exception {
        while (ips.next((rawEntry, offset, length) -> {
            Expected expect = expected.remove(0);
            System.out.println("key:" + SimpleRawEntry.key(rawEntry) + " vs" + expect.key + " value:" + SimpleRawEntry.value(rawEntry) + " vs " + expect.value);
            Assert.assertEquals(SimpleRawEntry.key(rawEntry), expect.key);
            Assert.assertEquals(SimpleRawEntry.value(rawEntry), expect.value);
            return true;
        }));
    }

    @Test
    public void testNext1() throws Exception {

        InterleavePointerStream ips = new InterleavePointerStream(new RawNextPointer[]{
            nextPointerSequence(new long[]{1, 2, 3, 4, 5}, new long[]{3, 3, 3, 3, 3}),
            nextPointerSequence(new long[]{1, 2, 3, 4, 5}, new long[]{2, 2, 2, 2, 2}),
            nextPointerSequence(new long[]{1, 2, 3, 4, 5}, new long[]{1, 1, 1, 1, 1})
        });

        List<Expected> expected = new ArrayList<>();
        expected.add(new Expected(1, 3));
        expected.add(new Expected(2, 3));
        expected.add(new Expected(3, 3));
        expected.add(new Expected(4, 3));
        expected.add(new Expected(5, 3));

        assertExpected(ips, expected);

    }

    @Test
    public void testNext2() throws Exception {

        InterleavePointerStream ips = new InterleavePointerStream(new RawNextPointer[]{
            nextPointerSequence(new long[]{10, 21, 29, 41, 50}, new long[]{1, 0, 0, 0, 1}),
            nextPointerSequence(new long[]{10, 21, 29, 40, 50}, new long[]{0, 0, 0, 1, 0}),
            nextPointerSequence(new long[]{10, 20, 30, 39, 50}, new long[]{0, 1, 1, 0, 0})
        });

        List<Expected> expected = new ArrayList<>();
        expected.add(new Expected(10, 1));
        expected.add(new Expected(20, 1));
        expected.add(new Expected(21, 0));
        expected.add(new Expected(29, 0));
        expected.add(new Expected(30, 1));
        expected.add(new Expected(39, 0));
        expected.add(new Expected(40, 1));
        expected.add(new Expected(41, 0));
        expected.add(new Expected(50, 1));

        assertExpected(ips, expected);

    }

    @Test
    public void testNext3() throws Exception {

        int count = 10;
        int step = 100;
        int indexes = 4;

        Random rand = new Random();

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        RawMemoryPointerIndex[] pointerIndexes = new RawMemoryPointerIndex[indexes];
        RawNextPointer[] nextPointers = new RawNextPointer[indexes];
        for (int wi = 0; wi < indexes; wi++) {

            int i = (indexes - 1) - wi;

            pointerIndexes[i] = new RawMemoryPointerIndex(new SimpleRawEntry());
            PointerIndexUtils.append(rand, pointerIndexes[i], 0, step, count, desired);
            System.out.println("Index " + i);
            RawNextPointer nextPointer = pointerIndexes[i].rawConcurrent(0).rowScan();
            while (nextPointer.next((rawEntry, offset, length) -> {
                System.out.println(SimpleRawEntry.toString(rawEntry));
                return true;
            }));
            System.out.println("\n");

            nextPointers[i] = pointerIndexes[i].rawConcurrent(0).rowScan();
        }

        InterleavePointerStream ips = new InterleavePointerStream(nextPointers);

        List<Expected> expected = new ArrayList<>();
        System.out.println("Expected:");
        for (Map.Entry<byte[], byte[]> entry : desired.entrySet()) {
            expected.add(new Expected(UIO.bytesLong(entry.getKey()), SimpleRawEntry.value(entry.getValue())));
            System.out.println(UIO.bytesLong(entry.getKey()) + " timestamp:" + SimpleRawEntry.value(entry.getValue()));
        }
        System.out.println("\n");

        assertExpected(ips, expected);

    }

    /*

     */
    static private class Expected {

        long key;
        long value;

        private Expected(long key, long value) {
            this.key = key;
            this.value = value;
        }

    }

    public RawNextPointer nextPointerSequence(long[] keys, long[] values) {
        int[] index = {0};
        return (stream) -> {
            if (index[0] < keys.length) {
                byte[] rawEntry = SimpleRawEntry.rawEntry(keys[index[0]], values[index[0]]);
                if (!stream.stream(rawEntry, 0, rawEntry.length)) {
                    return false;
                }
            }
            index[0]++;
            return index[0] <= keys.length;
        };
    }

}
