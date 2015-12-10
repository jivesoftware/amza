package com.jivesoftware.os.amza.lsm.lab;

import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;
import java.util.Comparator;

/**
 *
 * @author jonathan.colt
 */
public class InterpolationSearch {

    private static final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

    public static void main(String[] args) {
        byte[][] keys = new byte[][]{
            {0, 0, 1},
            {0, 1, 1},
            {0, 2, 1},
            {0, 3, 1},
            {0, 4, 1},
            {0, 5, 1},
            {0, 6, 1},
            {0, 7, 1},
            {1, 0, 1},
            {1, 1, 1},
            {1, 2, 1},
            {1, 3, 1},
            {2, 1, 1},
            {2, 2, 1},
            {2, 3, 1},
            {4, 0, 1},
            {4, 1, 1},
            {4, 2, 1},
            {4, 3, 1},
            {4, 4, 1},
            {4, 5, 1},
            {4, 6, 1},
            {4, 7, 1},
            {4, 8, 1},
            {4, 9, 1},
            {5, 0, 1},
            {5, 1, 1},
            {5, 2, 1},
            {6, 0, 1},
            {6, 1, 1},
            {6, 2, 1},
            {6, 3, 1},
            {6, 4, 1},
            {6, 5, 1},
            {6, 6, 1},
            {6, 7, 1},
            {6, 8, 1},
            {7, 0, 1},
            {7, 1, 1},
            {7, 2, 1},
            {7, 3, 1},
            {7, 4, 1},
            {8, 0, 1},
            {8, 0, 1},
            {8, 1, 1},
            {8, 2, 1},
            {8, 3, 1},
            {8, 4, 1},
            {8, 5, 1},
            {8, 6, 1},
            {8, 7, 1},
            {8, 8, 1},
            {8, 9, 1}
        };

        int[] prefixIndex = prefixIndex(keys);

        System.out.println("PrefixIndex:" + Arrays.toString(prefixIndex));
        double[] cdf = cdf(keys, 0);
        int[][] highLows = highLows(cdf, keys, 0);

        byte[] k = new byte[3];

        for (byte[] key : keys) {
            System.arraycopy(key, 0, k, 0, key.length);
            for (int i = 0; i < 3; i++) {
                k[2] = (byte) i;
                int bi = Arrays.binarySearch(keys, k, comparator);
                int is = interpolationSearch(highLows, keys, k, 0);
                int ps = prefixSearch(prefixIndex, keys, k);
                System.out.println("Ps:" + ps + " Bs:" + bi + " vs Is:" + is + " key:" + Arrays.toString(k));

            }

        }

        for (byte[] key : keys) {
            int[] highLow = highLows[key[0]];

            System.out.println("low:" + Arrays.toString(keys[highLow[1]]) + " key:" + Arrays.toString(key) + " high:" + Arrays.toString(keys[highLow[0]]));
        }

        if (1 + 1 == 2) {
            //return;
        }
        int run = 10_000_000;
        long start = System.currentTimeMillis();
        int c = 0;
        for (int i = 0; i < run; i++) {
            for (byte[] key : keys) {
                System.arraycopy(key, 0, k, 0, key.length);
                for (int j = 0; j < 3; j++) {
                    k[2] = (byte) j;
                    c += Arrays.binarySearch(keys, k, comparator);
                }
            }

        }
        System.out.println("BI:" + (System.currentTimeMillis() - start));

        for (int i = 0; i < run; i++) {
            for (byte[] key : keys) {
                System.arraycopy(key, 0, k, 0, key.length);
                for (int j = 0; j < 3; j++) {
                    k[2] = (byte) j;
                    c += prefixSearch(prefixIndex, keys, k);
                }
            }
        }
        System.out.println("PS:" + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        for (int i = 0; i < run; i++) {
            for (byte[] key : keys) {
                System.arraycopy(key, 0, k, 0, key.length);
                for (int j = 0; j < 3; j++) {
                    k[2] = (byte) j;
                    c += interpolationSearch(highLows, keys, k, 0);
                }
            }
        }
        System.out.println("IS:" + (System.currentTimeMillis() - start));

    }

    static int[] prefixIndex(byte[][] data) {
        int[] starts = new int[127];
        int start = 0;
        byte p = data[0][0];
        byte pi = 0;
        for (; pi < p; pi++) {
            starts[pi] = start - 1;
        }

        for (int di = 0; di < data.length; di++) {
            byte n = data[di][0];
            if (n > p) {
                for (int i = p; i < n; i++) {
                    starts[i] = start;
                }
                start = di;
                p = n;
            }
        }
        for (; p < starts.length; p++) {
            starts[p] = start;
        }
        return starts;
    }

    static int prefixSearch(int[] prefixIndex, byte[][] keys, byte[] key) {
        return Arrays.binarySearch(keys, prefixIndex[key[0]], prefixIndex[key[0] + 1], key, comparator);
    }

    static double[] cdf(byte[][] data, int level) {
        double[] freq = new double[128];
        double[] cdf = new double[128];
        for (byte[] d : data) {
            freq[d[level]]++;
        }
        cdf[0] = freq[0] / data.length;
        for (int i = 1; i < cdf.length; i++) {
            cdf[i] = cdf[i - 1] + freq[i] / data.length;
        }
        return cdf;
    }

    static int[][] highLows(double[] cdf, byte[][] keys, int level) {
        int[][] highLows = new int[cdf.length][2];

        for (byte[] key : keys) {
            double p = cdf[key[level]];
            double mean = (p * keys.length - 1);
            double sd = Math.sqrt((1 - p) * mean);

            int low = Math.max(0, (int) (mean - 1 * sd));
            int high = Math.min(keys.length - 1, (int) (mean + 1 * sd));

            highLows[key[level]][0] = high;
            highLows[key[level]][1] = low;
        }
        return highLows;
    }

    static int interpolationSearch(int[][] highLows, byte[][] keys, byte[] key, int level) {

        int[] highLow = highLows[key[level]];
        int low = highLow[1];
        int high = highLow[0];

        int c = comparator.compare(key, keys[low]);
        if (c == 0) {
            return low;
        } else if (c < 0) {
            return Arrays.binarySearch(keys, 0, low, key, comparator);
        } else {
            c = comparator.compare(keys[high], key);
            if (c == 0) {
                return high;
            } else if (c < 0) {
                return Arrays.binarySearch(keys, high, keys.length, key, comparator);
            } else {
                return Arrays.binarySearch(keys, low, high + 1, key, comparator);
            }
        }
    }
}
