package com.jivesoftware.os.amza.lsm;

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
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author jonathan.colt
 */
public class DiskBackedLeapPointerIndex implements RawConcurrentReadablePointerIndex, RawAppendablePointerIndex {

    public static final byte ENTRY = 0;
    public static final byte LEAP = 1;
    public static final byte BOUND = 2;
    public static final byte FOOTER = 3;

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
        int[] updatesSinceLeap = new int[1];

        byte[] lengthBuffer = new byte[4];
        long[] boundsFps = new long[updatesBetweenLeaps];
        HeapFiler entryBuffer = new HeapFiler(1024); // TODO somthing better

        byte[][] lastKey = new byte[1][];
        int[] leapCount = {0};
        long[] count = {0};

        pointers.consume((rawEntry, offset, length) -> {

            entryBuffer.reset();
            UIO.writeByte(entryBuffer, ENTRY, "type");

            int entryLength = 4 + length + 4;
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);
            entryBuffer.write(rawEntry, offset, length);
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);

            writeIndex.write(entryBuffer.leakBytes(), 0, (int) entryBuffer.length());

            int keyLength = UIO.bytesInt(rawEntry, offset);
            byte[] key = new byte[keyLength];
            System.arraycopy(rawEntry, 4, key, 0, keyLength);

            lastKey[0] = key;
            updatesSinceLeap[0]++;
            count[0]++;

            if (updatesSinceLeap[0] >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                entryBuffer.reset();
                latestLeapFrog[0] = writeLeaps(writeIndex, latestLeapFrog[0], leapCount[0], key, lengthBuffer);
                updatesSinceLeap[0] = 0;
                leapCount[0]++;
            }
            return true;
        });

        if (updatesSinceLeap[0] > 0) {
            writeIndex.write(entryBuffer.leakBytes(), 0, (int) entryBuffer.length());
            entryBuffer.reset();
            latestLeapFrog[0] = writeLeaps(writeIndex, latestLeapFrog[0], leapCount[0], lastKey[0], lengthBuffer);
            leapCount[0]++;
        }

        UIO.writeByte(writeIndex, FOOTER, "type");
        new Footer(leapCount[0], count[0]).write(writeIndex, lengthBuffer);
        writeIndex.flush(false);
        return true;
    }

    private LeapFrog writeLeaps(IWriteable writeIndex,
        LeapFrog latest,
        int index,
        byte[] key,
        byte[] lengthBuffer) throws IOException {

        Leaps leaps = computeNextLeaps(index, key, latest, maxLeaps);
        UIO.writeByte(writeIndex, LEAP, "type");
        long startOfLeapFp = writeIndex.getFilePointer();
        leaps.write(writeIndex, lengthBuffer);
        return new LeapFrog(startOfLeapFp, leaps);
    }

    Leaps leaps = null;
    //Map<Long, Leaps> leapsCache = Maps.newHashMap();
    TLongObjectHashMap<Leaps> leapsCache;
    Footer footer = null;

    @Override
    public RawReadPointerIndex rawConcurrent(int bufferSize) throws Exception {
        IReadable readableIndex = index.fileChannelMemMapFiler(0);
        if (readableIndex == null) {
            readableIndex = (bufferSize > 0) ? new HeapBufferedReadable(index.fileChannelFiler(), bufferSize) : index.fileChannelFiler();
        }

        if (footer == null) {
            byte[] lengthBuffer = new byte[8];
            long indexLength = readableIndex.length();
            if (indexLength < 4) {
                System.out.println("WTF:" + indexLength);
            }
            readableIndex.seek(indexLength - 4);
            int footerLength = UIO.readInt(readableIndex, "length", lengthBuffer);
            readableIndex.seek(indexLength - (footerLength + 1 + 4));
            int leapLength = UIO.readInt(readableIndex, "length", lengthBuffer);

            readableIndex.seek(indexLength - (1 + leapLength + 1 + footerLength));

            int type = readableIndex.read();
            if (type != LEAP) {
                throw new RuntimeException("Corruption! " + type + " expected " + LEAP);
            }
            leaps = Leaps.read(readableIndex, lengthBuffer);

            type = readableIndex.read();
            if (type != FOOTER) {
                throw new RuntimeException("Corruption! " + type + " expected " + FOOTER);
            }
            footer = Footer.read(readableIndex, lengthBuffer);
            leapsCache = new TLongObjectHashMap<>(footer.leapCount);

        }
        return new DiskBackedLeapReadablePointerIndex(leaps, readableIndex, leapsCache);
    }

    public static class DiskBackedLeapReadablePointerIndex implements RawReadPointerIndex {

        private final Leaps leaps;
        private final IReadable readable;
        private final TLongObjectHashMap<Leaps> leapsCache;
        private final byte[] lengthBuffer = new byte[8];

        public DiskBackedLeapReadablePointerIndex(Leaps leaps, IReadable readable, TLongObjectHashMap<Leaps> leapsCache) {
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
                if (at.fps.length != 0) {
                    int index = Arrays.binarySearch(at.keys, key, UnsignedBytes.lexicographicalComparator());
                    if (index == -(at.fps.length + 1)) {
                        closestFP = at.fps[at.fps.length - 1] - 1;
                    } else {
                        if (index < 0) {
                            index = -(index + 1);
                        }
                        next = leapsCache.get(at.fps[index]);
                        if (next == null) {
                            readable.seek(at.fps[index]);
                            next = Leaps.read(readable, lengthBuffer);
                            leapsCache.put(at.fps[index], next);
                        }
                    }
                }
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

        private class ActiveScan implements ScanFromFp {

            private long activeFp = Long.MAX_VALUE;

            @Override
            public boolean next(long fp, RawPointerStream stream) throws Exception {
                if (activeFp == Long.MAX_VALUE || activeFp != fp) {
                    activeFp = fp;
                    readable.seek(fp);
                }
                int type;
                while ((type = readable.read()) >= 0) {
                    if (type == ENTRY) {
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
                return type >= 0;
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

    private static class Bounds {

        private final byte[] fps;

        public Bounds(byte[] fps) {
            this.fps = fps;
        }

        private static Bounds read(IReadable readable, byte[] lengthBuffer) throws IOException {
            byte[] fps = UIO.readByteArray(readable, "fps", lengthBuffer);
            return new Bounds(fps);
        }

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

        private final int index;
        private final byte[] lastKey;
        private final long[] fps;
        private final byte[][] keys;

        public Leaps(int index, byte[] lastKey, long[] fpIndex, byte[][] keys) {
            this.index = index;
            this.lastKey = lastKey;
            this.fps = fpIndex;
            this.keys = keys;
        }

        private void write(IWriteable writeable, byte[] lengthBuffer) throws IOException {
            int entryLength = 4 + 4 + 4 + lastKey.length + 4;
            for (int i = 0; i < fps.length; i++) {
                entryLength += 8 + 4 + keys[i].length;
            }
            entryLength += 4;

            UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
            UIO.writeInt(writeable, index, "index", lengthBuffer);

            UIO.writeInt(writeable, lastKey.length, "lastKeyLength", lengthBuffer);
            UIO.write(writeable, lastKey, "lastKey");
            UIO.writeInt(writeable, fps.length, "fpIndexLength", lengthBuffer);

            for (int i = 0; i < fps.length; i++) {
                UIO.writeLong(writeable, fps[i], "fpIndex");
                UIO.writeByteArray(writeable, keys[i], "key", lengthBuffer);
            }
            UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
        }

        private static Leaps read(IReadable readable, byte[] lengthBuffer) throws IOException {
            int entryLength = UIO.readInt(readable, "entryLength", lengthBuffer);
            int index = UIO.readInt(readable, "index", lengthBuffer);
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
            return new Leaps(index, lastKey, fpIndex, keys);
        }
    }

    private static class Footer {

        private final int leapCount;
        private final long count;

        public Footer(int leapCount, long count) {
            this.leapCount = leapCount;
            this.count = count;
        }

        private void write(IWriteable writeable, byte[] lengthBuffer) throws IOException {
            int entryLength = 4 + 4 + 8 + 4;
            UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
            UIO.writeInt(writeable, leapCount, "leapCount", lengthBuffer);
            UIO.writeLong(writeable, count, "count");
            UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);

        }

        private static Footer read(IReadable readable, byte[] lengthBuffer) throws IOException {
            int entryLength = UIO.readInt(readable, "entryLength", lengthBuffer);
            int leapCount = UIO.readInt(readable, "leapCount", lengthBuffer);
            long count = UIO.readLong(readable, "count", lengthBuffer);
            if (UIO.readInt(readable, "entryLength", lengthBuffer) != entryLength) {
                throw new RuntimeException("Encountered length corruption. ");
            }
            return new Footer(leapCount, count);
        }
    }

    static private Leaps computeNextLeaps(int index, byte[] lastKey, LeapFrog latest, int maxLeaps) {
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
        return new Leaps(index, lastKey, fpIndex, keys);
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
