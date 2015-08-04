package com.jivesoftware.os.amza.shared.wal;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.Serializable;
import java.nio.ByteOrder;
import java.util.Comparator;
import sun.misc.Unsafe;

/**
 *
 */
public class KeyUtil {

    private static final BestComparator INSTANCE = new BestComparator();

    public static Comparator<byte[]> lexicographicalComparator() {
        return INSTANCE;
    }

    private static class BestComparator implements Comparator<byte[]>, Serializable {

        private static final Comparator<byte[]> BEST_COMPARATOR = getBestComparator();

        @Override
        public int compare(byte[] o1, byte[] o2) {
            return BEST_COMPARATOR.compare(o1, o2);
        }

        private static Comparator<byte[]> getBestComparator() {
            Comparison keyComparison = lexicographicalComparison();
            int PREFIX_OFFSET = 2;
            return (o1, o2) -> {
                short len1 = UIO.bytesShort(o1);
                short len2 = UIO.bytesShort(o2);

                int c = keyComparison.compare(o1, PREFIX_OFFSET, len1, o2, PREFIX_OFFSET, len2);
                if (c != 0) {
                    return c;
                }

                int keyOffset1 = PREFIX_OFFSET + len1;
                int keyOffset2 = PREFIX_OFFSET + len2;
                return keyComparison.compare(o1,
                    keyOffset1,
                    o1.length - keyOffset1,
                    o2,
                    keyOffset2,
                    o2.length - keyOffset2);
            };
        }
    }

    public interface Comparison {
        int compare(byte[] o1, int offset1, int len1, byte[] o2, int offset2, int len2);
    }

    public static Comparison lexicographicalComparison() {
        return Holder.BEST_COMPARISON;
    }

    private static class Holder {
        private static final String UNSAFE_COMPARATOR_NAME = Holder.class.getName() + "$UnsafeComparison";

        private static final Comparison BEST_COMPARISON = getBestComparison();

        private static Comparison getBestComparison() {
            try {
                Class<?> theClass = Class.forName(UNSAFE_COMPARATOR_NAME);

                @SuppressWarnings("unchecked")
                Comparison comparison = (Comparison) theClass.getEnumConstants()[0];
                return comparison;
            } catch (Throwable t) { // ensure we really catch *everything*
                return lexicographicalComparisonJavaImpl();
            }
        }

        private enum UnsafeComparison implements Comparison {
            SINGLETON;

            private final boolean littleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
            private final int UNSIGNED_MASK = 0xFF;

           /*
            * The following static final fields exist for performance reasons.
            *
            * In UnsignedBytesBenchmark, accessing the following objects via static
            * final fields is the fastest (more than twice as fast as the Java
            * implementation, vs ~1.5x with non-final static fields, on x86_32)
            * under the Hotspot server compiler. The reason is obviously that the
            * non-final fields need to be reloaded inside the loop.
            *
            * And, no, defining (final or not) local variables out of the loop still
            * isn't as good because the null check on the theUnsafe object remains
            * inside the loop and BYTE_ARRAY_BASE_OFFSET doesn't get
            * constant-folded.
            *
            * The compiler can treat static final fields as compile-time constants
            * and can constant-fold them while (final or not) local variables are
            * run time values.
            */

            static final Unsafe theUnsafe;

            /** The offset to the first element in a byte array. */
            static final int BYTE_ARRAY_BASE_OFFSET;

            static {
                theUnsafe = getUnsafe();

                BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

                // sanity check - this should never fail
                if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
                    throw new AssertionError();
                }
            }

            /**
             * Returns a sun.misc.Unsafe.  Suitable for use in a 3rd party package.
             * Replace with a simple call to Unsafe.getUnsafe when integrating
             * into a jdk.
             *
             * @return a sun.misc.Unsafe
             */
            private static sun.misc.Unsafe getUnsafe() {
                try {
                    return sun.misc.Unsafe.getUnsafe();
                } catch (SecurityException tryReflectionInstead) {
                }
                try {
                    return java.security.AccessController.doPrivileged((java.security.PrivilegedExceptionAction<Unsafe>) () -> {
                        Class<Unsafe> k = Unsafe.class;
                        for (java.lang.reflect.Field f : k.getDeclaredFields()) {
                            f.setAccessible(true);
                            Object x = f.get(null);
                            if (k.isInstance(x)) {
                                return k.cast(x);
                            }
                        }
                        throw new NoSuchFieldError("the Unsafe");
                    });
                } catch (java.security.PrivilegedActionException e) {
                    throw new RuntimeException("Could not initialize intrinsics",
                        e.getCause());
                }
            }

            @Override
            public int compare(byte[] o1, int offset1, int len1, byte[] o2, int offset2, int len2) {
                int minLength = Math.min(len1, len2);
                int minWords = minLength / Longs.BYTES;

                /*
                 * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
                 * time is no slower than comparing 4 bytes at a time even on 32-bit.
                 * On the other hand, it is substantially faster on 64-bit.
                 */
                for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
                    long lw = theUnsafe.getLong(o1, BYTE_ARRAY_BASE_OFFSET + offset1 + (long) i);
                    long rw = theUnsafe.getLong(o2, BYTE_ARRAY_BASE_OFFSET + offset2 + (long) i);
                    long diff = lw ^ rw;

                    if (diff != 0) {
                        if (!littleEndian) {
                            return UnsignedLongs.compare(lw, rw);
                        }

                        // Use binary search
                        int n = 0;
                        int y;
                        int x = (int) diff;
                        if (x == 0) {
                            x = (int) (diff >>> 32);
                            n = 32;
                        }

                        y = x << 16;
                        if (y == 0) {
                            n += 16;
                        } else {
                            x = y;
                        }

                        y = x << 8;
                        if (y == 0) {
                            n += 8;
                        }
                        return (int) (((lw >>> n) & UNSIGNED_MASK) - ((rw >>> n) & UNSIGNED_MASK));
                    }
                }

                // The epilogue to cover the last (minLength % 8) elements.
                for (int i = minWords * Longs.BYTES; i < minLength; i++) {
                    int result = UnsignedBytes.compare(o1[offset1 + i], o2[offset2 + i]);
                    if (result != 0) {
                        return result;
                    }
                }
                return len1 - len2;
            }
        }

        private static Comparison lexicographicalComparisonJavaImpl() {
            return PureJavaComparison.INSTANCE;
        }

        private enum PureJavaComparison implements Comparison {
            INSTANCE;

            @Override
            public int compare(byte[] o1, int offset1, int len1, byte[] o2, int offset2, int len2) {
                int minLength = Math.min(len1, len2);
                for (int i = 0; i < minLength; i++) {
                    int result = UnsignedBytes.compare(o1[offset1 + i], o2[offset2 + i]);
                    if (result != 0) {
                        return result;
                    }
                }
                return len1 - len2;
            }
        }
    }
}
