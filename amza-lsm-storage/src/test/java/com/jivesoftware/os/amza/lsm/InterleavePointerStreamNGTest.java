package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
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

        InterleavePointerStream ips = new InterleavePointerStream(new NextPointer[]{
            nextPointerSequence(new long[]{1, 2, 3, 4, 5}, new long[]{3, 3, 3, 3, 3})
        });

        List<Expected> expected = new ArrayList<>();
        expected.add(new Expected(1, 3));
        expected.add(new Expected(2, 3));
        expected.add(new Expected(3, 3));
        expected.add(new Expected(4, 3));
        expected.add(new Expected(5, 3));

        while (ips.next((key, timestamp, tombstoned, version, pointer) -> {
            Expected expect = expected.remove(0);
            System.out.println("key:" + UIO.bytesLong(key) + " vs" + expect.key + " value:" + timestamp + " vs " + expect.value);
            Assert.assertEquals(UIO.bytesLong(key), expect.key);
            Assert.assertEquals(timestamp, expect.value);
            return true;
        }));
    }

    @Test
    public void testNext1() throws Exception {

        InterleavePointerStream ips = new InterleavePointerStream(new NextPointer[]{
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

        while (ips.next((key, timestamp, tombstoned, version, pointer) -> {
            Expected expect = expected.remove(0);
            System.out.println("key:" + UIO.bytesLong(key) + " vs" + expect.key + " value:" + timestamp + " vs " + expect.value);
            Assert.assertEquals(UIO.bytesLong(key), expect.key);
            Assert.assertEquals(timestamp, expect.value);
            return true;
        }));
    }

    @Test
    public void testNext2() throws Exception {

        InterleavePointerStream ips = new InterleavePointerStream(new NextPointer[]{
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

        while (ips.next((key, timestamp, tombstoned, version, pointer) -> {
            Expected expect = expected.remove(0);
            System.out.println("key:" + UIO.bytesLong(key) + " vs" + expect.key + " value:" + timestamp + " vs " + expect.value);
            Assert.assertEquals(UIO.bytesLong(key), expect.key);
            Assert.assertEquals(timestamp, expect.value);
            return true;
        }));
    }

    @Test
    public void testNext3() throws Exception {

        int count = 10;
        int step = 100;
        int indexes = 4;

        Random rand = new Random();

        ConcurrentSkipListMap<byte[], TimestampedValue> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        MemoryPointerIndex[] pointerIndexes = new MemoryPointerIndex[indexes];
        NextPointer[] nextPointers = new NextPointer[indexes];
        for (int wi = 0; wi < indexes; wi++) {

            int i = (indexes - 1) - wi;

            pointerIndexes[i] = new MemoryPointerIndex();
            PointerIndexUtils.append(rand, pointerIndexes[i], 0, step, count, desired);
            System.out.println("Index " + i);
            NextPointer nextPointer = pointerIndexes[i].concurrent(0).rowScan();
            while (nextPointer.next((key, timestamp, tombstoned, version, pointer) -> {
                System.out.println(UIO.bytesLong(key) + " timestamp:" + timestamp);
                return true;
            }));
            System.out.println("\n");

            nextPointers[i] = pointerIndexes[i].concurrent(0).rowScan();
        }

        InterleavePointerStream ips = new InterleavePointerStream(nextPointers);

        List<Expected> expected = new ArrayList<>();
        System.out.println("Expected:");
        for (Map.Entry<byte[], TimestampedValue> entry : desired.entrySet()) {
            expected.add(new Expected(UIO.bytesLong(entry.getKey()), entry.getValue().getTimestampId()));
            System.out.println(UIO.bytesLong(entry.getKey()) + " timestamp:" + entry.getValue().getTimestampId());
        }
        System.out.println("\n");

        while (ips.next((key, timestamp, tombstoned, version, pointer) -> {
            Expected expect = expected.remove(0);
            System.out.println("key:" + UIO.bytesLong(key) + " vs" + expect.key + " value:" + timestamp + " vs " + expect.value);
            Assert.assertEquals(UIO.bytesLong(key), expect.key);
            Assert.assertEquals(timestamp, expect.value);
            return true;
        }));

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

    public NextPointer nextPointerSequence(long[] keys, long[] values) {
        int[] index = {0};
        return (stream) -> {
            if (index[0] < keys.length) {
                if (!stream.stream(UIO.longBytes(keys[index[0]]), values[index[0]], true, 0, 0)) {
                    return false;
                }
            }
            index[0]++;
            return index[0] <= keys.length;
        };
    }

}
