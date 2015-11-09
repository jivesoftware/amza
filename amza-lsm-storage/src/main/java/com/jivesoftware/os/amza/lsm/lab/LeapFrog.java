package com.jivesoftware.os.amza.lsm.lab;

/**
 *
 * @author jonathan.colt
 */
class LeapFrog {

    final long fp;
    final Leaps leaps;

    public LeapFrog(long fp, Leaps leaps) {
        this.fp = fp;
        this.leaps = leaps;
    }

    static public Leaps computeNextLeaps(int index, byte[] lastKey, LeapFrog latest, int maxLeaps, long[] startOfEntryIndex) {
        long[] fpIndex;
        byte[][] keys;
        if (latest == null) {
            fpIndex = new long[0];
            keys = new byte[0][];
        } else if (latest.leaps.fps.length < maxLeaps) {
            int numLeaps = latest.leaps.fps.length + 1;
            fpIndex = new long[numLeaps];
            keys = new byte[numLeaps][];
            System.arraycopy(latest.leaps.fps, 0, fpIndex, 0, latest.leaps.fps.length);
            System.arraycopy(latest.leaps.keys, 0, keys, 0, latest.leaps.keys.length);
            fpIndex[numLeaps - 1] = latest.fp;
            keys[numLeaps - 1] = latest.leaps.lastKey;
        } else {
            fpIndex = new long[0];
            keys = new byte[maxLeaps][];

            long[] idealFpIndex = new long[maxLeaps];
            // b^n = fp
            // b^32 = 123_456
            // ln b^32 = ln 123_456
            // 32 ln b = ln 123_456
            // ln b = ln 123_456 / 32
            // b = e^(ln 123_456 / 32)
            double base = Math.exp(Math.log(latest.fp) / maxLeaps);
            for (int i = 0; i < idealFpIndex.length; i++) {
                idealFpIndex[i] = latest.fp - (long) Math.pow(base, (maxLeaps - i - 1));
            }

            double smallestDistance = Double.MAX_VALUE;
            for (int i = 0; i < latest.leaps.fps.length; i++) {
                long[] testFpIndex = new long[maxLeaps];

                System.arraycopy(latest.leaps.fps, 0, testFpIndex, 0, i);
                System.arraycopy(latest.leaps.fps, i + 1, testFpIndex, i, maxLeaps - 1 - i);
                testFpIndex[maxLeaps - 1] = latest.fp;

                double distance = euclidean(testFpIndex, idealFpIndex);
                if (distance < smallestDistance) {
                    fpIndex = testFpIndex;
                    System.arraycopy(latest.leaps.keys, 0, keys, 0, i);
                    System.arraycopy(latest.leaps.keys, i + 1, keys, i, maxLeaps - 1 - i);
                    keys[maxLeaps - 1] = latest.leaps.lastKey;
                    smallestDistance = distance;
                }
            }
        }
        return new Leaps(index, lastKey, fpIndex, keys, startOfEntryIndex);
    }

    static private double euclidean(long[] a, long[] b) {
        double v = 0;
        for (int i = 0; i < a.length; i++) {
            long d = a[i] - b[i];
            v += d * d;
        }
        return Math.sqrt(v);
    }
}
