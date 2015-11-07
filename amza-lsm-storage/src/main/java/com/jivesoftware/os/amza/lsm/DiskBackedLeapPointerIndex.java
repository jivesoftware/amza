package com.jivesoftware.os.amza.lsm;

import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.RawAppendablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.RawConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.RawNextPointer;
import com.jivesoftware.os.amza.lsm.api.RawPointGet;
import com.jivesoftware.os.amza.lsm.api.RawPointerStream;
import com.jivesoftware.os.amza.lsm.api.RawPointers;
import com.jivesoftware.os.amza.lsm.api.RawReadPointerIndex;
import com.jivesoftware.os.amza.lsm.api.ScanFromFp;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class DiskBackedLeapPointerIndex implements RawConcurrentReadablePointerIndex, RawAppendablePointerIndex {

    private final int maxLeaps;
    private final int updatesBetweenLeaps;

    private final DiskBackedPointerIndexFiler index;
    private byte[] minKey;
    private byte[] maxKey;

    public DiskBackedLeapPointerIndex(DiskBackedPointerIndexFiler index, int maxLeaps, int updatesBetweenLeaps) {
        this.index = index;
        this.maxLeaps = maxLeaps;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
    }

    @Override
    public void destroy() throws IOException {
        // TODO aquireAll?
        close();

        new File(index.getFileName()).delete();
    }

    @Override
    public void close() throws IOException {
        // TODO aquireAll?
        index.close();
    }

    @Override
    public boolean append(RawPointers pointers) throws Exception {
        IWriteable writeIndex = index.fileChannelWriter();

        writeIndex.seek(0);

        LeapFrog[] latestLeapFrog = new LeapFrog[1];
        long[] updatesSinceLeap = new long[1];

        byte[] lengthBuffer = new byte[4];
        HeapFiler indexEntryFiler = new HeapFiler(1024); // TODO somthing better

        byte[][] lastKey = new byte[1][];
        pointers.consume((rawEntry, offset, length) -> {

            indexEntryFiler.reset();
            UIO.writeByte(indexEntryFiler, (byte) 0, "type");

            int entryLength = 4 + length + 4;
            UIO.writeInt(indexEntryFiler, entryLength, "entryLength", lengthBuffer);
            indexEntryFiler.write(rawEntry, offset, length);
            UIO.writeInt(indexEntryFiler, entryLength, "entryLength", lengthBuffer);

            writeIndex.write(indexEntryFiler.leakBytes(), 0, (int) indexEntryFiler.length());

            int keyLength = UIO.bytesInt(rawEntry, offset);
            byte[] key = new byte[keyLength];
            System.arraycopy(rawEntry, 4, key, 0, keyLength);
            lastKey[0] = key;
            updatesSinceLeap[0]++;
            if (updatesSinceLeap[0] >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                latestLeapFrog[0] = writeLeaps(writeIndex, latestLeapFrog[0], key, lengthBuffer);
                updatesSinceLeap[0] = 0;
            }
            return true;
        });

        if (updatesSinceLeap[0] > 0) {
            latestLeapFrog[0] = writeLeaps(writeIndex, latestLeapFrog[0], lastKey[0], lengthBuffer);
        }
        writeIndex.flush(false);
        return true;
    }

    private LeapFrog writeLeaps(IWriteable writeIndex,
        LeapFrog latest,
        byte[] key,
        byte[] lengthBuffer) throws IOException {

        Leaps leaps = computeNextLeaps(key, latest, maxLeaps);
        UIO.writeByte(writeIndex, (byte) 1, "type");
        long startOfLeapFp = writeIndex.getFilePointer();
        leaps.write(writeIndex, lengthBuffer);
        return new LeapFrog(startOfLeapFp, leaps);
    }

    Leaps leaps = null;
    Map<Long, Leaps> leapsCache = Maps.newHashMap();

    @Override
    public RawReadPointerIndex rawConcurrent(int bufferSize) throws Exception {
        IReadable readableIndex = index.fileChannelMemMapFiler(0);
        if (readableIndex == null) {
            readableIndex = (bufferSize > 0) ? new HeapBufferedReadable(index.fileChannelFiler(), bufferSize) : index.fileChannelFiler();
        }

        if (leaps == null) {
            byte[] lengthBuffer = new byte[8];
            long indexLength = readableIndex.length();
            if (indexLength < 4) {
                System.out.println("WTF:" + indexLength);
            }
            readableIndex.seek(indexLength - 4);
            int length = UIO.readInt(readableIndex, "length", lengthBuffer);
            readableIndex.seek(indexLength - length);
            leaps = Leaps.read(readableIndex, lengthBuffer);
        }
        return new DiskBackedLeapReadablePointerIndex(leaps, readableIndex, leapsCache);
    }

    public static class DiskBackedLeapReadablePointerIndex implements RawReadPointerIndex {

        private final Leaps leaps;
        private final IReadable readable;
        private final Map<Long, Leaps> leapsCache;
        private final byte[] lengthBuffer = new byte[8];

        public DiskBackedLeapReadablePointerIndex(Leaps leaps, IReadable readable, Map<Long, Leaps> leapsCache) {
            this.leaps = leaps;
            this.readable = readable;
            this.leapsCache = leapsCache;
        }

        private static class LeapPointerStream implements RawPointerStream {

            private byte[] desired;
            private RawPointerStream stream;
            private boolean once = false;
            private boolean result = false;

            private void prepare(byte[] desired, RawPointerStream stream) {
                this.desired = desired;
                this.stream = stream;
                this.once = false;
                this.result = false;
            }

            @Override
            public boolean stream(byte[] rawEntry, int offset, int length) throws Exception {
                int keylength = UIO.bytesInt(rawEntry, 0);
                int c = PointerIndexUtil.compare(rawEntry, 4, keylength, desired, 0, desired.length);

                if (c == 0) {
                    if (rawEntry != null) {
                        result = stream.stream(rawEntry, offset, length);
                    } else {
                        result = false;
                    }
                    once = true;
                    return result;
                }
                if (c > 0) {
                    once = true;
                    return false;
                } else {
                    return true;
                }
            }
        }

        @Override
        public RawPointGet getPointer() throws Exception {
            return new Gets(new ActiveScan());
        }

        private class Gets implements RawPointGet {

            private byte[] activeKey;
            private long activeFp;
            private ScanFromFp scanFromFp;
            private final LeapPointerStream leapPointerStream = new LeapPointerStream();

            public Gets(ScanFromFp scanFromFp) {
                this.scanFromFp = scanFromFp;
            }

            @Override
            public boolean next(byte[] key, RawPointerStream stream) throws Exception {
                if (activeKey == null || activeKey != key) {
                    activeKey = key;
                    activeFp = getInclusiveStartOfRow(key);
                    if (activeFp < 0) {
                        reset();
                        return false;
                    }
                    leapPointerStream.prepare(activeKey, stream);
                }
                if (leapPointerStream.once) {
                    return false;
                }
                while (scanFromFp.next(activeFp, leapPointerStream)) ;
                boolean more = leapPointerStream.once && leapPointerStream.result;
                if (!more) {
                    reset();
                }
                return more;
            }

            private void reset() {
                activeKey = null;
            }
        }

        public long getInclusiveStartOfRow(byte[] key) throws Exception {
            Leaps at = leaps;
            if (UnsignedBytes.lexicographicalComparator().compare(leaps.lastKey, key) < 0) {
                return -1;
            }

            long closestFP = 0;
            while (at != null) {
                Leaps next = null;

                // Binary Search
                if (at.fpIndex.length != 0) {
                    int index = Arrays.binarySearch(at.keys, key, UnsignedBytes.lexicographicalComparator());
                    if (index == -(at.fpIndex.length + 1)) {
                        closestFP = at.fpIndex[at.fpIndex.length - 1] - 1;
                    } else {
                        if (index < 0) {
                            index = -(index + 1);
                        }
                        next = leapsCache.get(at.fpIndex[index]);
                        if (next == null) {
                            readable.seek(at.fpIndex[index]);
                            next = Leaps.read(readable, lengthBuffer);
                            leapsCache.put(at.fpIndex[index], next);
                        }
                    }
                }

                // Brute forcce
//                for (int i = 0; i < at.keys.length; i++) {
//                    if (UnsignedBytes.lexicographicalComparator().compare(at.keys[i], key) < 0) {
//                        closestFP = Math.max(closestFP, at.fpIndex[i] - 1);
//                    } else {
//                        next = leapsCache.get(at.fpIndex[i]);
//                        if (next == null) {
//                            readable.seek(at.fpIndex[i]);
//                            next = Leaps.read(readable, lengthBuffer);
//                            leapsCache.put(at.fpIndex[i], next);
//                        }
//
//                        break;
//                    }
//                }
                at = next;
            }
            return closestFP;
        }

        @Override
        public RawNextPointer rangeScan(byte[] from, byte[] to) throws Exception {
            long fp = getInclusiveStartOfRow(from);
            if (fp < 0) {
                return stream -> false;
            }
            ScanFromFp scanFromFp = new ActiveScan();
            return (stream) -> {
                boolean[] once = new boolean[]{false};
                boolean more = true;
                while (!once[0] && more) {
                    more = scanFromFp.next(fp, (rawEntry, offset, length) -> {
                        int keylength = UIO.bytesInt(rawEntry, offset);
                        int c = PointerIndexUtil.compare(rawEntry, 4, keylength, from, 0, from.length);
                        if (c >= 0) {
                            c = PointerIndexUtil.compare(rawEntry, 4, keylength, to, 0, to.length);
                            if (c < 0) {
                                once[0] = true;
                            }
                            return c < 0 && stream.stream(rawEntry, offset, length);
                        } else {
                            return true;
                        }
                    });
                }
                return more;
            };
        }

        @Override
        public RawNextPointer rowScan() throws Exception {
            ScanFromFp scanFromFp = new ActiveScan();
            return (stream) -> scanFromFp.next(0, stream);
        }

        private byte[] entryBuffer;
        private int entryLength;

     
        class ActiveScan implements ScanFromFp {

            private long activeFp = Long.MAX_VALUE;

            @Override
            public boolean next(long fp, RawPointerStream stream) throws Exception {
                if (activeFp == Long.MAX_VALUE || activeFp != fp) {
                    activeFp = fp;
                    readable.seek(fp);
                }
                int type;
                while ((type = readable.read()) >= 0) {
                    if (type == 0) {
                        int length = UIO.readInt(readable, "entryLength", lengthBuffer);
                        entryLength = length - 4;
                        if (entryBuffer == null || entryBuffer.length < entryLength) {
                            entryBuffer = new byte[entryLength];
                        }
                        readable.read(entryBuffer, 0, entryLength);
                        return stream.stream(entryBuffer, 0, entryLength);
                    } else {
                        int length = UIO.readInt(readable, "entryLength", lengthBuffer);
                        readable.seek(readable.getFilePointer() + (length - 4));
                    }
                }
                boolean more = type >= 0;
                return more;
            }

        }

        @Override
        public void close() throws Exception {
        }

        @Override
        public long count() throws Exception {
            return -1;// TODO
        }

        @Override
        public boolean isEmpty() throws Exception {
            return readable.length() == 0;
        }

    }

    @Override
    public boolean isEmpty() throws IOException {
        return index.length() == 0;
    }

    @Override
    public long count() throws IOException {
        return 0; // TODO I hate counts
    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public String toString() {
        return "DiskBackedLeapPointerIndex{" + "index=" + index + ", minKey=" + minKey + ", maxKey=" + maxKey + '}';

    }

    private static class LeapFrog {

        private final long fp;
        private final Leaps leaps;

        public LeapFrog(long fp, Leaps leaps) {
            this.fp = fp;
            this.leaps = leaps;
        }
    }

    private static class Leaps {

        private final byte[] lastKey;
        private final long[] fpIndex;
        private final byte[][] keys;

        public Leaps(byte[] lastKey, long[] fpIndex, byte[][] keys) {
            this.lastKey = lastKey;
            this.fpIndex = fpIndex;
            this.keys = keys;
        }

        private void write(IWriteable writeable, byte[] lengthBuffer) throws IOException {
            int entryLength = 4 + 4 + lastKey.length + 4;
            for (int i = 0; i < fpIndex.length; i++) {
                entryLength += 8 + 4 + keys[i].length;
            }
            entryLength += 4;

            UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
            UIO.writeInt(writeable, lastKey.length, "lastKeyLength", lengthBuffer);
            UIO.write(writeable, lastKey, "lastKey");
            UIO.writeInt(writeable, fpIndex.length, "fpIndexLength", lengthBuffer);

            for (int i = 0; i < fpIndex.length; i++) {
                UIO.writeLong(writeable, fpIndex[i], "fpIndex");
                UIO.writeByteArray(writeable, keys[i], "key", lengthBuffer);
            }
            UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
        }

        private static Leaps read(IReadable readable, byte[] lengthBuffer) throws IOException {
            int entryLength = UIO.readInt(readable, "entryLength", lengthBuffer);
            int lastKeyLength = UIO.readInt(readable, "lastKeyLength", lengthBuffer);
            byte[] lastKey = new byte[lastKeyLength];
            UIO.read(readable, lastKey);
            int fpIndexLength = UIO.readInt(readable, "fpIndexLength", lengthBuffer);
            long[] fpIndex = new long[fpIndexLength];
            byte[][] keys = new byte[fpIndexLength][];
            for (int i = 0; i < fpIndexLength; i++) {
                fpIndex[i] = UIO.readLong(readable, "fpIndex", lengthBuffer);
                keys[i] = UIO.readByteArray(readable, "keyLength", lengthBuffer);
            }
            if (UIO.readInt(readable, "entryLength", lengthBuffer) != entryLength) {
                throw new RuntimeException("Encountered length corruption. ");
            }
            return new Leaps(lastKey, fpIndex, keys);
        }
    }

    static private Leaps computeNextLeaps(byte[] lastKey, LeapFrog latest, int maxLeaps) {
        long[] fpIndex;
        byte[][] keys;
        if (latest == null) {
            fpIndex = new long[0];
            keys = new byte[0][];
        } else if (latest.leaps.fpIndex.length < maxLeaps) {
            int numLeaps = latest.leaps.fpIndex.length + 1;
            fpIndex = new long[numLeaps];
            keys = new byte[numLeaps][];
            System.arraycopy(latest.leaps.fpIndex, 0, fpIndex, 0, latest.leaps.fpIndex.length);
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

            for (int i = 0; i < latest.leaps.fpIndex.length; i++) {
                long[] testFpIndex = new long[maxLeaps];

                System.arraycopy(latest.leaps.fpIndex, 0, testFpIndex, 0, i);
                System.arraycopy(latest.leaps.fpIndex, i + 1, testFpIndex, i, maxLeaps - 1 - i);
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

            //System.out.println("@" + latest.fp + " base:  " + base);
            //System.out.println("@" + latest.fp + " ideal: " + Arrays.toString(idealFpIndex));
            //System.out.println("@" + latest.fp + " next:  " + Arrays.toString(fpIndex));
        }

        return new Leaps(lastKey, fpIndex, keys);
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
