package com.jivesoftware.os.amza.lsm.lab;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import com.jivesoftware.os.amza.lsm.lab.api.ScanFromFp;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @author jonathan.colt
 */
public class ReadLeapsAndBoundsIndex implements ReadIndex {

    private final Leaps leaps;
    private final IReadable readable;
    private final TLongObjectHashMap<Leaps> leapsCache;
    private final byte[] lengthBuffer = new byte[8];

    public ReadLeapsAndBoundsIndex(Leaps leaps, IReadable readable, TLongObjectHashMap<Leaps> leapsCache) {
        this.leaps = leaps;
        this.readable = readable;
        this.leapsCache = leapsCache;
    }

    @Override
    public GetRaw get() throws Exception {
        return new Gets(new ActiveScan(readable, lengthBuffer));
    }

    public class Gets implements GetRaw, RawEntryStream {

        private final ScanFromFp scanFromFp;

        private RawEntryStream activeStream;
        private boolean found = false;

        public Gets(ScanFromFp scanFromFp) {
            this.scanFromFp = scanFromFp;
        }

        @Override
        public boolean get(byte[] key, RawEntryStream stream) throws Exception {
            //TODO pass an IBA since this needs to be treated as immutable

            long activeFp = getInclusiveStartOfRow(key, true);
            if (activeFp < 0) {
                return false;
            }

            activeStream = stream;
            found = false;
            scanFromFp.reset();

            boolean more = true;
            while (more && !found) {
                more = scanFromFp.next(activeFp, this);
            }
            return found;
        }

        @Override
        public boolean stream(byte[] rawEntry, int offset, int length) throws Exception {
            boolean result = activeStream.stream(rawEntry, offset, length);
            found = true;
            return result;
        }

        @Override
        public boolean result() {
            return scanFromFp.result();
        }
    }

    public long getInclusiveStartOfRow(byte[] key, boolean exact) throws Exception {
        Leaps at = leaps;
        if (UnsignedBytes.lexicographicalComparator().compare(leaps.lastKey, key) < 0) {
            return -1;
        }
        while (at != null) {
            Leaps next = null;
            int index = Arrays.binarySearch(at.keys, key, UnsignedBytes.lexicographicalComparator());
            if (index == -(at.fps.length + 1)) {
                /*if (at.fps.length == 0) {
                    return 0;
                }
                return at.fps[at.fps.length - 1] - 1;*/
                return binarySearchClosestFP(at, key, exact);
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
            at = next;
        }
        return -1;
    }

    private long binarySearchClosestFP(Leaps at, byte[] key, boolean exact) throws IOException {
        int low = 0;
        int high = at.startOfEntryIndex.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long fp = at.startOfEntryIndex[mid];

            readable.seek(fp);
            byte[] midKey = UIO.readByteArray(readable, "key", lengthBuffer);
            if (midKey == null) {
                throw new IllegalStateException("Missing key");
            }

            int cmp = IndexUtil.compare(midKey, 0, midKey.length, key, 0, key.length);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return fp - (1 + 4); // key found. (1 for type 4 for entry length)
            }
        }
        if (exact) {
            return -1;
        } else {
            return at.startOfEntryIndex[low] - (1 + 4); // best index. (1 for type 4 for entry length)
        }
    }

    public static int interpolationSearch(byte[][] list, byte[] x) {

        Comparator<byte[]> cmp = UnsignedBytes.lexicographicalComparator();

        int l = 0;
        int r = list.length - 1;

        while (l <= r) {
            if (list[l] == list[r]) {
                if (cmp.compare(list[l], x) == 0) {
                    return l;
                } else {
                    return -1;// not found
                }
            }

            int k = hammingDistance(x, list[l]) / hammingDistance(list[r], list[l]);

            // not found
            if (k < 0 || k > 1) {
                return -1;
            }

            int mid = (l + k * (r - l));

            int midc = cmp.compare(x, list[mid]);
            if (midc < 0) {
                r = mid - 1;
            } else if (midc > 0) {
                l = mid + 1;
            } else {
                return mid;// success!
            }
        }
        return -1;// not found

    }

    private static int hammingDistance(byte[] x, byte[] y) {
        if (x.length != y.length) {
            throw new IllegalArgumentException(String.format("Arrays have different length: x[%d], y[%d]", x.length, y.length));
        }

        int dist = 0;
        for (int i = 0; i < x.length; i++) {
            if (x[i] != y[i]) {
                dist++;
            }
        }

        return dist;
    }

    @Override
    public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
        long fp = getInclusiveStartOfRow(from, false);
        if (fp < 0) {
            return (stream) -> false;
        }
        ScanFromFp scanFromFp = new ActiveScan(readable, lengthBuffer);
        return (stream) -> {
            boolean[] once = new boolean[] { false };
            boolean more = true;
            while (!once[0] && more) {
                more = scanFromFp.next(fp,
                    (rawEntry, offset, length) -> {
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
            return scanFromFp.result();
        };
    }

    @Override
    public NextRawEntry rowScan() throws Exception {
        ScanFromFp scanFromFp = new ActiveScan(readable, lengthBuffer);
        return (stream) -> {
            scanFromFp.next(0, stream);
            return scanFromFp.result();
        };
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
