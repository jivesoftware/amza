package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerStream;
import com.jivesoftware.os.amza.lsm.api.ReadPointerIndex;
import java.io.IOException;

import static com.jivesoftware.os.amza.lsm.DiskBackedPointerIndex.INDEX_ENTRY_SIZE;
import static com.jivesoftware.os.amza.lsm.DiskBackedPointerIndex.KEY_FP;
import static com.jivesoftware.os.amza.lsm.DiskBackedPointerIndex.KEY_LENGTH;
import static com.jivesoftware.os.amza.lsm.DiskBackedPointerIndex.SORT_INDEX;
import static com.jivesoftware.os.amza.lsm.DiskBackedPointerIndex.TIMESTAMP;
import static com.jivesoftware.os.amza.lsm.DiskBackedPointerIndex.TOMBSTONE;
import static com.jivesoftware.os.amza.lsm.DiskBackedPointerIndex.VERSION;
import static com.jivesoftware.os.amza.lsm.DiskBackedPointerIndex.WAL_POINTER;

/**
 *
 * @author jonathan.colt
 */
public class ReadablePointerIndex implements ReadPointerIndex {

    private final int count;
    private final byte[] minKey;
    private final byte[] maxKey;
    private final IReadable readableIndex;
    private final IReadable readableKeys;
    private final long[] offsetAndLength = new long[2];

    ReadablePointerIndex(int count,
        byte[] minKey,
        byte[] maxKey,
        IReadable readableIndex,
        IReadable readableKeys) {
        this.count = count;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.readableIndex = readableIndex;
        this.readableKeys = readableKeys;
    }

    public static byte[] readKeyAtIndex(int i, IReadable index, IReadable keys) throws IOException {
        byte[] intLongBuffer = new byte[8];

        index.seek(i * INDEX_ENTRY_SIZE);
        int sortIndex = UIO.readInt(index, "sortIndex", intLongBuffer);
        long keyFp = UIO.readLong(index, "keyFp", intLongBuffer);
        int keyLength = UIO.readInt(index, "keyLength", intLongBuffer);

        keys.seek(keyFp + 4);
        byte[] key = new byte[keyLength];
        keys.read(key);
        return key;
    }

    @Override
    public NextPointer getPointer(byte[] key) throws Exception {
        if (UnsignedBytes.lexicographicalComparator().compare(minKey, key) > 0) {
            //System.out.println("Key to small to be in the index. minKey:" + Arrays.toString(minKey) + " key:" + Arrays.toString(key));
            return (PointerStream stream) -> stream.stream(-(0 + 1), key, -1, false, -1, -1);
        } else if (UnsignedBytes.lexicographicalComparator().compare(key, maxKey) > 0) {
            //System.out.println("Key to large to be in the index. maxKey:" + Arrays.toString(maxKey) + " key:" + Arrays.toString(key));
            return (PointerStream stream) -> stream.stream(-(count + 1), key, -1, false, -1, -1);
        } else {
            return binarySearch(0, count, key, null, new byte[8]);
        }
    }

    @Override
    public NextPointer rangeScan(byte[] from, byte[] to) throws Exception {
        return binarySearch(0, count, from, to, new byte[8]);
    }

    @Override
    public NextPointer rowScan() throws Exception {
        byte[] intLongBuffer = new byte[8];
        int[] i = new int[]{0};
        return (stream) -> {
            if (i[0] < count) {
                readableIndex.seek(i[0] * INDEX_ENTRY_SIZE);
                int sortIndex = UIO.readInt(readableIndex, "sortIndex", intLongBuffer);
                long keyFp = UIO.readLong(readableIndex, "keyFp", intLongBuffer);
                int keyLength = UIO.readInt(readableIndex, "keyLength", intLongBuffer);
                long timestamp = UIO.readLong(readableIndex, "timestamp", intLongBuffer);
                boolean tombstone = UIO.readBoolean(readableIndex, "tombstone");
                long version = UIO.readLong(readableIndex, "version", intLongBuffer);
                long walPointerFp = UIO.readLong(readableIndex, "walPointerFp", intLongBuffer);
                byte[] key = new byte[keyLength];

                readableKeys.seek(keyFp + 4);
                readableKeys.read(key);
                i[0]++;
                return stream.stream(Integer.MAX_VALUE, key, timestamp, tombstone, version, walPointerFp);
            } else {
                return false;
            }
        };
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    private void fillOffsetAndLength(int index, byte[] intLongBuffer) throws Exception {
        readableIndex.seek((index * INDEX_ENTRY_SIZE) + SORT_INDEX);
        offsetAndLength[0] = UIO.readLong(readableIndex, "keyFp", intLongBuffer);
        offsetAndLength[1] = UIO.readInt(readableIndex, "keyLength", intLongBuffer);
    }

    // TODO add reverse order support if toKey < fromKey
    private NextPointer binarySearch(int fromIndex, int toIndex, byte[] fromKey, byte[] toKey, byte[] intLongBuffer) throws Exception {
        byte[] ttvfp = new byte[TIMESTAMP + TOMBSTONE + VERSION + WAL_POINTER];

        int low = fromIndex;
        int high = toIndex - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            fillOffsetAndLength(mid, intLongBuffer);
            int cmp = compare(fromKey, intLongBuffer);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else // return mid;  key found
            {
                if (toKey == null) {
                    int _mid = mid;
                    int[] i = new int[]{0};
                    return (stream) -> {
                        if (i[0] == 0) {
                            readableIndex.seek((_mid * INDEX_ENTRY_SIZE) + SORT_INDEX + KEY_FP + KEY_LENGTH);
                            readableIndex.read(ttvfp);
                            i[0]++;
                            return stream.stream(_mid,
                                fromKey,
                                UIO.bytesLong(ttvfp, 0),
                                UIO.bytesBoolean(ttvfp, TIMESTAMP),
                                UIO.bytesLong(ttvfp, TIMESTAMP + TOMBSTONE),
                                UIO.bytesLong(ttvfp, TIMESTAMP + TOMBSTONE + VERSION));
                        } else {
                            return false;
                        }
                    };
                } else {
                    int[] i = new int[]{0};
                    return (stream) -> {
                        if (mid + i[0] < count) {
                            boolean more = stream(mid + i[0], toKey, stream, intLongBuffer);
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
                    return stream.stream(-(_low + 1), fromKey, -1, false, -1, -1);
                } else {
                    return false;
                }
            };
        } else {
            int _low = low;
            int[] i = new int[]{0};
            return (stream) -> {
                if (_low + i[0] < count) {
                    boolean more = stream(_low + i[0], toKey, stream, intLongBuffer);
                    i[0]++;
                    return more;
                } else {
                    return false;
                }
            };
        }
    }

    private boolean stream(int i, byte[] stopKeyExclusive, PointerStream stream, byte[] intLongBuffer) throws Exception {
        readableIndex.seek(i * INDEX_ENTRY_SIZE);
        int sortIndex = UIO.readInt(readableIndex, "sortIndex", intLongBuffer);
        long keyFp = UIO.readLong(readableIndex, "keyFp", intLongBuffer);
        int keyLength = UIO.readInt(readableIndex, "keyLength", intLongBuffer);
        long timestamp = UIO.readLong(readableIndex, "timestamp", intLongBuffer);
        boolean tombstone = UIO.readBoolean(readableIndex, "tombstone");
        long version = UIO.readLong(readableIndex, "version", intLongBuffer);
        long walPointerFp = UIO.readLong(readableIndex, "walPointerFp", intLongBuffer);
        byte[] key = new byte[keyLength];

        readableKeys.seek(keyFp);
        UIO.readInt(readableKeys, "keyLength", intLongBuffer);
        readableKeys.read(key);

        if (UnsignedBytes.lexicographicalComparator().compare(key, stopKeyExclusive) < 0) {
            return stream.stream(i, key, timestamp, tombstone, version, walPointerFp);
        } else {
            return false;
        }
    }

    // UnsighedBytes lex compare
    private int compare(byte[] right, byte[] intLongBuffer) throws IOException {
        readableKeys.seek(offsetAndLength[0]);
        byte[] left = new byte[(int) offsetAndLength[1]];
        UIO.readInt(readableKeys, "keyLength", intLongBuffer);
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
