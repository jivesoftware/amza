package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.RawAppendablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.RawNextPointer;
import com.jivesoftware.os.amza.lsm.api.RawPointerStream;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;

import static com.jivesoftware.os.amza.lsm.SimpleRawEntry.key;
import static com.jivesoftware.os.amza.lsm.SimpleRawEntry.rawEntry;
import static com.jivesoftware.os.amza.lsm.SimpleRawEntry.value;

/**
 *
 * @author jonathan.colt
 */
public class PointerIndexUtils {

    static OrderIdProvider timeProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));

    static long append(Random rand,
        RawAppendablePointerIndex appendablePointerIndex,
        long start,
        int step,
        int count,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws Exception {

        long[] lastKey = new long[1];
        appendablePointerIndex.append((stream) -> {
            long k = start + rand.nextInt(step);
            for (int i = 0; i < count; i++) {
                byte[] key = UIO.longBytes(k);
                long time = timeProvider.nextId();

                long specialK = k;
                if (desired != null) {
                    desired.compute(key, (t, u) -> {
                        if (u == null) {
                            return rawEntry(specialK, time);
                        } else {
                            return value(u) > time ? u : rawEntry(specialK, time);
                        }
                    });
                }
                byte[] rawEntry = rawEntry(k, time);
                if (!stream.stream(rawEntry, 0, rawEntry.length)) {
                    break;
                }
                k += 1 + rand.nextInt(step);
            }
            lastKey[0] = k;
            return true;
        });
        return lastKey[0];
    }

    static void assertions(MergeablePointerIndexs indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        RawNextPointer rowScan = indexs.rowScan();
        RawPointerStream stream = (rawEntry, offset, length) -> {
            System.out.println("scanned:" + UIO.bytesLong(keys.get(index[0])) + " " + key(rawEntry));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), key(rawEntry));
            index[0]++;
            return true;
        };
        while (rowScan.next(stream));

        System.out.println("rowScan PASSED");

        for (int i = 0; i < count * step; i++) {
            long k = i;
            RawNextPointer getPointer = indexs.getPointer(UIO.longBytes(k));
            stream = (rawEntry, offset, length) -> {
                byte[] expectedFP = desired.get(UIO.longBytes(key(rawEntry)));
                if (expectedFP == null) {
                    Assert.assertTrue(expectedFP == null && value(rawEntry) == -1);
                } else {
                    Assert.assertEquals(value(expectedFP), value(rawEntry));
                }
                return true;
            };

            while (getPointer.next(stream));
        }

        System.out.println("getPointer PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (rawEntry, offset, length) -> {
                if (value(rawEntry) > -1) {
                    System.out.println("Streamed:" + key(rawEntry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            RawNextPointer rangeScan = indexs.rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        System.out.println("rangeScan PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (rawEntry, offset, length) -> {
                if (value(rawEntry) > -1) {
                    streamed[0]++;
                }
                return true;
            };
            RawNextPointer rangeScan = indexs.rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(2, streamed[0]);

        }

        System.out.println("rangeScan2 PASSED");
    }

}
