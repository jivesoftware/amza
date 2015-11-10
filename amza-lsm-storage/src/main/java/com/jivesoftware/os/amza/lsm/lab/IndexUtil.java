package com.jivesoftware.os.amza.lsm.lab;

import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import sun.misc.Unsafe;

/**
 * @author jonathan.colt
 */
public class IndexUtil {

    private static class PointGetRaw implements GetRaw {

        private final GetRaw[] pointGets;
        private boolean result;

        public PointGetRaw(GetRaw[] pointGets) {
            this.pointGets = pointGets;
        }

        @Override
        public boolean get(byte[] key, RawEntryStream stream) throws Exception {
            for (GetRaw pointGet : pointGets) {
                if (pointGet.get(key, stream)) {
                    result = pointGet.result();
                    return result;
                }
            }
            result = stream.stream(null, -1, -1);
            return result;
        }

        @Override
        public boolean result() {
            return result;
        }
    }

    public static GetRaw get(RawConcurrentReadableIndex[] indexes) throws Exception {

        GetRaw[] pointGets = new GetRaw[indexes.length];
        for (int i = 0; i < pointGets.length; i++) {
            ReadIndex rawConcurrent = indexes[i].rawConcurrent(2_048);
            pointGets[i] = rawConcurrent.get();
        }

        return new PointGetRaw(pointGets);
    }

    public static NextRawEntry rangeScan(RawConcurrentReadableIndex[] copy, byte[] from, byte[] to) throws Exception {
        NextRawEntry[] feeders = new NextRawEntry[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].rawConcurrent(4096).rangeScan(from, to);
        }
        return new InterleaveStream(feeders);
    }

    public static NextRawEntry rowScan(RawConcurrentReadableIndex[] copy) throws Exception {
        NextRawEntry[] feeders = new NextRawEntry[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].rawConcurrent(1024 * 1024 * 10).rowScan();
        }
        return new InterleaveStream(feeders);
    }

    /**
     * Borrowed from guava.
     */

    static final boolean BIG_ENDIAN;
    static final Unsafe theUnsafe;
    static final int BYTE_ARRAY_BASE_OFFSET;

    static {
        BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
        theUnsafe = getUnsafe();
        if (theUnsafe != null) {
            BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
            if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
                throw new AssertionError();
            }
        } else {
            BYTE_ARRAY_BASE_OFFSET = -1;
        }
    }

    private static Unsafe getUnsafe() {
        try {
            return Unsafe.getUnsafe();
        } catch (SecurityException var2) {
            try {
                return (Unsafe) AccessController.doPrivileged((PrivilegedExceptionAction) () -> {
                    Class k = Unsafe.class;
                    Field[] arr$ = k.getDeclaredFields();
                    int len$ = arr$.length;

                    for (int i$ = 0; i$ < len$; ++i$) {
                        Field f = arr$[i$];
                        f.setAccessible(true);
                        Object x = f.get((Object) null);
                        if (k.isInstance(x)) {
                            return (Unsafe) k.cast(x);
                        }
                    }
                    return null;
                });
            } catch (PrivilegedActionException var1) {
                return null;
            }
        } catch (Throwable t) {
            return null;
        }
    }

    public static int compare(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        if (theUnsafe != null) {
            return compareNative(left, leftOffset, leftLength, right, rightOffset, rightLength);
        } else {
            return comparePure(left, leftOffset, leftLength, right, rightOffset, rightLength);
        }
    }

    private static int compareNative(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        int minLength = Math.min(leftLength, rightLength);
        int minWords = minLength / 8;

        int i;
        for (i = 0; i < minWords * 8; i += 8) {
            long result = theUnsafe.getLong(left, (long) BYTE_ARRAY_BASE_OFFSET + leftOffset + (long) i);
            long rw = theUnsafe.getLong(right, (long) BYTE_ARRAY_BASE_OFFSET + rightOffset + (long) i);
            if (result != rw) {
                if (BIG_ENDIAN) {
                    return UnsignedLongs.compare(result, rw);
                }

                int n = Long.numberOfTrailingZeros(result ^ rw) & -8;
                return (int) ((result >>> n & 255L) - (rw >>> n & 255L));
            }
        }

        for (i = minWords * 8; i < minLength; ++i) {
            int var11 = UnsignedBytes.compare(left[leftOffset + i], right[rightOffset + i]);
            if (var11 != 0) {
                return var11;
            }
        }

        return leftLength - rightLength;
    }

    private static int comparePure(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        int minLength = Math.min(leftLength, rightLength);
        for (int i = 0; i < minLength; i++) {
            int result = (left[leftOffset + i] & 0xFF) - (right[rightOffset + i] & 0xFF);
            if (result != 0) {
                return result;
            }
        }
        return leftLength - rightLength;
    }
}
