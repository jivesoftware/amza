package com.jivesoftware.os.amza.lsm.lab;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.ScanFromFp;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.util.Arrays;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;

/**
 *
 * @author jonathan.colt
 */
public class ReadLeapAndBoundsIndex implements ReadIndex {

    private final Leaps leaps;
    private final IReadable readable;
    private final TLongObjectHashMap<Leaps> leapsCache;
    private final byte[] lengthBuffer = new byte[8];

    public ReadLeapAndBoundsIndex(Leaps leaps, IReadable readable, TLongObjectHashMap<Leaps> leapsCache) {
        this.leaps = leaps;
        this.readable = readable;
        this.leapsCache = leapsCache;
    }

    private static class LeapPointerStream implements RawEntryStream {

        private byte[] desired;
        private RawEntryStream stream;
        private boolean once = false;
        private boolean result = false;

        private void prepare(byte[] desired, RawEntryStream stream) {
            this.desired = desired;
            this.stream = stream;
            this.once = false;
            this.result = false;
        }

        @Override
        public boolean stream(byte[] rawEntry, int offset, int length) throws Exception {
            int keylength = UIO.bytesInt(rawEntry, 0);
            int c = IndexUtil.compare(rawEntry, 4, keylength, desired, 0, desired.length);
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
    public GetRaw get() throws Exception {
        return new Gets(new ActiveScan());
    }

    public class Gets implements GetRaw {

        private byte[] activeKey;
        private long activeFp;
        private ScanFromFp scanFromFp;
        private final LeapPointerStream leapPointerStream = new LeapPointerStream();

        public Gets(ScanFromFp scanFromFp) {
            this.scanFromFp = scanFromFp;
        }

        @Override
        public boolean next(byte[] key, RawEntryStream stream) throws Exception {
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
            while (scanFromFp.next(activeFp, leapPointerStream)) {
                ;
            }
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
    public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
        long fp = getInclusiveStartOfRow(from);
        if (fp < 0) {
            return (stream) -> false;
        }
        ScanFromFp scanFromFp = new ActiveScan();
        return (com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream stream) -> {
            boolean[] once = new boolean[]{false};
            boolean more = true;
            while (!once[0] && more) {
                more = scanFromFp.next(fp,
                    (byte[] rawEntry, int offset, int length) -> {
                    int keylength = UIO.bytesInt(rawEntry, offset);
                    int c = IndexUtil.compare(rawEntry, 4, keylength, from, 0, from.length);
                    if (c >= 0) {
                        c = IndexUtil.compare(rawEntry, 4, keylength, to, 0, to.length);
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
    public NextRawEntry rowScan() throws Exception {
        ScanFromFp scanFromFp = new ActiveScan();
        return (stream) -> scanFromFp.next(0, stream);
    }
    private byte[] entryBuffer;
    private int entryLength;

    private class ActiveScan implements ScanFromFp {

        private long activeFp = Long.MAX_VALUE;

        @Override
        public boolean next(long fp, RawEntryStream stream) throws Exception {
            if (activeFp == Long.MAX_VALUE || activeFp != fp) {
                activeFp = fp;
                readable.seek(fp);
            }
            int type;
            while ((type = readable.read()) >= 0) {
                if (type == LeapsAndBoundsIndex.ENTRY) {
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
        return -1; // TODO
    }

    @Override
    public boolean isEmpty() throws Exception {
        return readable.length() == 0;
    }

}
