package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerIndex;
import com.jivesoftware.os.amza.lsm.api.PointerStream;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class ReadablePointerIndex implements PointerIndex {
    private final int count;
    private final IReadable readableIndex;
    private final IReadable readableKeys;
    private final long[] offsetAndLength = new long[2];

    ReadablePointerIndex(int count, IReadable readableIndex, IReadable readableKeys) {
        this.count = count;
        this.readableIndex = readableIndex;
        this.readableKeys = readableKeys;
    }

    @Override
    public NextPointer getPointer(byte[] key) throws Exception {
        return binarySearch(0, count, key, null);
    }

    @Override
    public NextPointer rangeScan(byte[] from, byte[] to) throws Exception {
        return binarySearch(0, count, from, to);
    }

    @Override
    public NextPointer rowScan() throws Exception {
        int[] i = new int[]{0};
        return (stream) -> {
            if (i[0] < count) {
                readableIndex.seek(i[0] * DiskBackedPointerIndex.INDEX_ENTRY_SIZE);
                int sortIndex = UIO.readInt(readableIndex, "sortIndex");
                long keyFp = UIO.readLong(readableIndex, "keyFp");
                int keyLength = UIO.readInt(readableIndex, "keyLength");
                long timestamp = UIO.readLong(readableIndex, "timestamp");
                boolean tombstone = UIO.readBoolean(readableIndex, "tombstone");
                long walPointerFp = UIO.readLong(readableIndex, "walPointerFp");
                byte[] key = new byte[keyLength];
                readableKeys.seek(keyFp);
                readableKeys.read(key);
                i[0]++;
                return stream.stream(Integer.MAX_VALUE, key, timestamp, tombstone, walPointerFp);
            } else {
                return false;
            }
        };
    }

    public int count() {
        return count;
    }

    private void fillOffsetAndLength(int index) throws Exception {
        readableIndex.seek(index * DiskBackedPointerIndex.INDEX_ENTRY_SIZE);
        int sortIndex = UIO.readInt(readableIndex, "sortIndex");
        offsetAndLength[0] = UIO.readLong(readableIndex, "keyFp");
        offsetAndLength[1] = UIO.readInt(readableIndex, "keyLength");
    }

    // TODO add reverse order support if toKey < fromKey
    private NextPointer binarySearch(int fromIndex, int toIndex, byte[] fromKey, byte[] toKey) throws Exception {
        int low = fromIndex;
        int high = toIndex - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            fillOffsetAndLength(mid);
            int cmp = compare(fromKey);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                // return mid;  key found
                if (toKey == null) {
                    int _mid = mid;
                    int[] i = new int[]{0};
                    return (stream) -> {
                        if (i[0] == 0) {
                            readableIndex.seek((_mid * DiskBackedPointerIndex.INDEX_ENTRY_SIZE) + 4 + 8 + 4);
                            i[0]++;
                            return stream.stream(_mid, fromKey, UIO.readLong(readableIndex, "timestamp"), UIO.readBoolean(readableIndex, "tombstone"),
                                UIO.readLong(readableIndex, "walPointerFp"));
                        } else {
                            return false;
                        }
                    };
                } else {
                    int[] i = new int[]{0};
                    return (stream) -> {
                        if (mid + i[0] < count) {
                            boolean more = stream(mid + i[0], toKey, stream);
                            i[0]++;
                            return more;
                        } else {
                            return false;
                        }
                    };
                }
            }
        }
        // return -(low + 1);  // key not found.
        if (toKey == null) {
            int _low = low;
            int[] i = new int[]{0};
            return (stream) -> {
                if (i[0] == 0) {
                    i[0]++;
                    return stream.stream(-(_low + 1), fromKey, -1, false, -1);
                } else {
                    return false;
                }
            };
        } else {
            int _low = low;
            int[] i = new int[]{0};
            return (stream) -> {
                if (_low + i[0] < count) {
                    boolean more = stream(_low + i[0], toKey, stream);
                    i[0]++;
                    return more;
                } else {
                    return false;
                }
            };
        }
    }

    private boolean stream(int i, byte[] stopKeyExclusive, PointerStream stream) throws Exception {
        readableIndex.seek(i * DiskBackedPointerIndex.INDEX_ENTRY_SIZE);
        int sortIndex = UIO.readInt(readableIndex, "sortIndex");
        long keyFp = UIO.readLong(readableIndex, "keyFp");
        int keyLength = UIO.readInt(readableIndex, "keyLength");
        long timestamp = UIO.readLong(readableIndex, "timestamp");
        boolean tombstone = UIO.readBoolean(readableIndex, "tombstone");
        long walPointerFp = UIO.readLong(readableIndex, "walPointerFp");
        byte[] key = new byte[keyLength];
        readableKeys.seek(keyFp);
        readableKeys.read(key);
        if (UnsignedBytes.lexicographicalComparator().compare(key, stopKeyExclusive) < 0) {
            return stream.stream(i, key, timestamp, tombstone, walPointerFp);
        } else {
            return false;
        }
    }

    // UnsighedBytes lex compare
    private int compare(byte[] right) throws IOException {
        readableKeys.seek(offsetAndLength[0]);
        byte[] left = new byte[(int) offsetAndLength[1]];
        readableKeys.read(left);
        int minLength = Math.min(left.length, right.length);
        for (int i = 0; i < minLength; i++) {
            int result = UnsignedBytes.compare(left[i], right[i]);
            if (result != 0) {
                return result;
            }
        }
        return left.length - right.length;
    }

}
