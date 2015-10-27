package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.AppendablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerStream;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;

/**
 *
 * @author jonathan.colt
 */
public class PointerIndexUtils {

    static final Random rand = new Random();
    static OrderIdProvider timeProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));

    static void append(AppendablePointerIndex appendablePointerIndex,
        long start,
        int step,
        int count,
        ConcurrentSkipListMap<byte[], TimestampedValue> desired) throws Exception {

        appendablePointerIndex.append((stream) -> {
            long k = start + rand.nextInt(step);
            for (int i = 0; i < count; i++) {
                long walFp = Math.abs(rand.nextLong());
                byte[] key = UIO.longBytes(k);
                long time = timeProvider.nextId();
                if (desired != null) {
                    desired.compute(key, (t, u) -> {
                        if (u == null) {
                            return new TimestampedValue(time, Long.MAX_VALUE, UIO.longBytes(walFp));
                        } else {
                            return u.getTimestampId() > time ? u : new TimestampedValue(time, Long.MAX_VALUE, UIO.longBytes(walFp));
                        }
                    });
                }
                if (!stream.stream(Integer.MIN_VALUE, key, time, false, 0, walFp)) {
                    break;
                }
                k += 1 + rand.nextInt(step);
            }
            return true;
        });
    }

    static void assertions(MergeablePointerIndexs indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], TimestampedValue> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        NextPointer rowScan = indexs.rowScan();
        PointerStream stream = (sortIndex, key, timestamp, tombstoned, version, fp) -> {
            System.out.println("scanned:" + UIO.bytesLong(keys.get(index[0])) + " " + UIO.bytesLong(key));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), UIO.bytesLong(key));
            index[0]++;
            return true;
        };
        while (rowScan.next(stream));

        System.out.println("rowScan PASSED");

        for (int i = 0; i < count * step; i++) {
            long k = i;
            NextPointer getPointer = indexs.getPointer(UIO.longBytes(k));
            stream = (sortIndex, key, timestamp, tombstoned, version, fp) -> {
                TimestampedValue expectedFP = desired.get(key);
                if (expectedFP == null) {
                    Assert.assertTrue(expectedFP == null && fp == -1);
                } else {
                    Assert.assertEquals(UIO.bytesLong(expectedFP.getValue()), fp);
                }
                return true;
            };

            while (getPointer.next(stream));
        }

        System.out.println("getPointer PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (sortIndex, key, timestamp, tombstoned, version, fp) -> {
                if (fp > -1) {
                    System.out.println("Streamed:" + UIO.bytesLong(key));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            NextPointer rangeScan = indexs.rangeScan(keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(3, streamed[0]);

        }

        System.out.println("rangeScan PASSED");

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (sortIndex, key, timestamp, tombstoned, version, fp) -> {
                if (fp > -1) {
                    streamed[0]++;
                }
                return true;
            };
            NextPointer rangeScan = indexs.rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(2, streamed[0]);

        }

        System.out.println("rangeScan2 PASSED");
    }
}
