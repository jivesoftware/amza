package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawAppendableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;

import static com.jivesoftware.os.amza.lsm.lab.SimpleRawEntry.key;
import static com.jivesoftware.os.amza.lsm.lab.SimpleRawEntry.rawEntry;
import static com.jivesoftware.os.amza.lsm.lab.SimpleRawEntry.value;

/**
 * @author jonathan.colt
 */
public class IndexTestUtils {

    static OrderIdProvider timeProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));

    static public long append(Random rand,
        RawAppendableIndex appendablePointerIndex,
        long start,
        int step,
        int count,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws Exception {

        long[] lastKey = new long[1];
        appendablePointerIndex.append((stream) -> {
            long k = start;
            for (int i = 0; i < count; i++) {
                k += 1 + rand.nextInt(step);

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
            }
            lastKey[0] = k;
            return true;
        });
        return lastKey[0];
    }

    static public void assertions(MergeableIndexes indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        NextRawEntry rowScan = indexs.rowScan();
        RawEntryStream stream = (rawEntry, offset, length) -> {
            System.out.println("scanned:" + UIO.bytesLong(keys.get(index[0])) + " " + key(rawEntry));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), key(rawEntry));
            index[0]++;
            return true;
        };
        while (rowScan.next(stream)) ;

        System.out.println("rowScan PASSED");

        for (int i = 0; i < count * step; i++) {
            long k = i;
            GetRaw getPointer = indexs.get();
            stream = (rawEntry, offset, length) -> {
                byte[] expectedFP = desired.get(UIO.longBytes(key(rawEntry)));
                if (expectedFP == null) {
                    Assert.assertTrue(expectedFP == null && value(rawEntry) == -1);
                } else {
                    Assert.assertEquals(value(expectedFP), value(rawEntry));
                }
                return true;
            };

            while (getPointer.get(UIO.longBytes(k), stream)) ;
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
            NextRawEntry rangeScan = indexs.rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream)) ;
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
            NextRawEntry rangeScan = indexs.rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream)) ;
            Assert.assertEquals(2, streamed[0]);

        }

        System.out.println("rangeScan2 PASSED");
    }

}
