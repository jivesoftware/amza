package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.shared.wal.WALTx;
import java.lang.ref.WeakReference;

/**
 *
 * @author jonathan.colt
 */
public class ImmutableWALPSortedBlockReader<I> {

    private final WALTx<I> walTx;
    private final long keysFP;
    private final long pointersFP;

    private WeakReference<Object[]> weakKeys;
    private WeakReference<byte[]> weakPointer;

    public ImmutableWALPSortedBlockReader(WALTx<I> walTx, long keysFP, long pointersFP) {
        this.walTx = walTx;
        this.keysFP = keysFP;
        this.pointersFP = pointersFP;
    }

    public long getKeysFP() {
        return keysFP;
    }

    public long getPointersFP() {
        return pointersFP;
    }
/*
    public static <I> ImmutableWALPSortedBlockReader<I> write(WALTx<I> walTx, long txId, WALKeyPointerStream stream) throws Exception {
        return walTx.write((WALWriter writer) -> {
            writer.write(txId, RowType.system, (WALWriter.RawRowStream stream) -> {
                stream.stream(pointers);
                stream.stream(pointers);

            }, ew, null);
            return new ImmutableWALPSortedBlockReader(walTx, keysFP, pointersFP);
        });
    }

    public Object[] hydrateKeys() throws Exception {
        Object[] got = weakKeys == null ? null : weakKeys.get();
        if (got == null) {
            got = walTx.read((WALReader reader) -> {
                byte[] rawKeys = reader.read(keysFP);
                HeapFiler filer = new HeapFiler(rawKeys);
                int count = UIO.readInt(filer, "count");
                Object[] keys = new Object[count];
                for (int i = 0; i < count; i++) {
                    keys[i] = UIO.readByteArray(filer, "key");
                }
                return keys;
            });
            weakKeys = new WeakReference<>(got);
        }
        return got;
    }

    public byte[] hydratePointer() throws Exception {
        byte[] got = weakPointer == null ? null : weakPointer.get();
        if (got == null) {
            got = walTx.read((WALReader reader) -> {
                return reader.read(pointersFP);
            });
            weakPointer = new WeakReference<>(got);
        }
        return got;
    }

    private static int binarySearch(int[] boundaryIndexs, byte[] a, int fromIndex, int toIndex, byte[] key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midValIndex = boundaryIndexs[mid];
            int midValIndexLength = (midValIndex + 1 < boundaryIndexs.length) ? boundaryIndexs[midValIndex + 1] - midValIndex : a.length - midValIndex;
            //int cmp = UnsignedBytes.lexicographicalComparator().compare(midVal, key);
            int cmp = compare(a, midValIndex, midValIndexLength, key, 0, key.length);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1);  // key not found.
    }

    // UnsighedBytes lex compare
    private static int compare(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        int minLength = Math.min(leftLength, rightLength);
        for (int i = 0; i < minLength; i++) {
            int result = UnsignedBytes.compare(left[leftOffset + i], right[rightOffset + i]);
            if (result != 0) {
                return result;
            }
        }
        return leftLength - rightLength;
    }

    private final int pointerSize = 8 + 1 + 8;

    private boolean streamPointer(byte[] pointers, int i, byte[] key, KeyContainedStream stream) throws Exception {
        int offset = i * pointerSize;
        return stream.stream(key, pointers[offset + 8] == 1);
    }

    private boolean streamPointer(byte[] pointers, int i, byte[] key, WALKeyPointerStream stream) throws Exception {
        int offset = i * pointerSize;
        return stream.stream(key, UIO.bytesLong(pointers, offset), pointers[offset + 8] == 1, UIO.bytesLong(pointers, offset + 9));
    }

    private boolean streamPointer(byte[] pointers, int i, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned,
        WALKeyValuePointerStream stream) throws Exception {
        int offset = i * pointerSize;
        return stream.stream(key, value, valueTimestamp, valueTombstoned,
            UIO.bytesLong(pointers, offset), pointers[offset + 8] == 1, UIO.bytesLong(pointers, offset + 9));
    }

    public boolean getPointer(byte[] key, WALKeyPointerStream stream) throws Exception {
        Object[] keys = hydrateKeys();
        int i = binarySearch(keys, 0, keys.length, key);
        if (i >= 0) {
            byte[] pointers = hydratePointer();
            return streamPointer(pointers, i, key, stream);
        }
        return true;
    }

    // expects walKeys to be in lex order.
    public boolean getPointers(Object[] walKeys, WALKeyPointerStream stream) throws Exception {
        Object[] keys = hydrateKeys();
        byte[] pointers = hydratePointer();
        return walKeys.consume(new WALKeyStream() {
            int lastI = 0;

            @Override
            public boolean stream(byte[] key) throws Exception {
                if (lastI >= keys.length) {
                    return stream.stream(key, -1, false, -1);
                }
                int i = binarySearch(keys, lastI, keys.length, key);
                if (i >= 0) {
                    if (!streamPointer(pointers, i, key, stream)) {
                        return false;
                    }
                } else {
                    if (!stream.stream(key, -1, false, -1)) {
                        return false;
                    }
                }
                lastI = Math.abs(i + 1);
                return true;
            }
        });
    }

    public boolean getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        Object[] keys = hydrateKeys();
        byte[] pointers = hydratePointer();
        return keyValues.consume(new KeyValueStream() {
            int lastI = 0;

            @Override
            public boolean stream(byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception {
                if (lastI >= keys.length) {
                    return stream.stream(key, value, valueTimestamp, valueTombstoned, -1, false, -1);
                }
                int i = binarySearch(keys, lastI, keys.length, key);
                if (i >= 0) {
                    if (!streamPointer(pointers, i, key, value, valueTimestamp, valueTombstoned, stream)) {
                        return false;
                    }
                } else {
                    if (!stream.stream(key, value, valueTimestamp, valueTombstoned, -1, false, -1)) {
                        return false;
                    }
                }
                lastI = Math.abs(i + 1);
                return true;
            }
        });
    }

    public boolean containsKeys(WALKeys walKeys, KeyContainedStream stream) throws Exception {
        Object[] keys = hydrateKeys();
        byte[] pointers = hydratePointer();
        return walKeys.consume(new WALKeyStream() {
            int lastI = 0;

            @Override
            public boolean stream(byte[] key) throws Exception {
                if (lastI >= keys.length) {
                    return stream.stream(key, false);
                }
                int i = binarySearch(keys, lastI, keys.length, key);
                if (i >= 0) {
                    if (!streamPointer(pointers, i, key, stream)) {
                        return false;
                    }
                } else {
                    if (!stream.stream(key, false)) {
                        return false;
                    }
                }
                lastI = Math.abs(i + 1);
                return true;
            }
        });
    }

    public boolean isEmpty() throws Exception {
        Object[] keys = hydrateKeys();
        return keys.length == 0;
    }

    public long size() throws Exception {
        Object[] keys = hydrateKeys();
        return keys.length;
    }

    public boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
        Object[] keys = hydrateKeys();
        byte[] pointers = hydratePointer();
        int f = binarySearch(keys, 0, keys.length, from);
        if (Math.abs(f + 1) >= keys.length) {
            return true;
        }
        int t = binarySearch(keys, f, keys.length, from);
        for (int i = f; i < t; i++) {
            if (!streamPointer(pointers, i, (byte[]) keys[i], stream)) {
                return false;
            }
        }
        return true;
    }

    public boolean rowScan(WALKeyPointerStream stream) throws Exception {
        Object[] keys = hydrateKeys();
        byte[] pointers = hydratePointer();
        for (int i = 0; i < keys.length; i++) {
            if (!streamPointer(pointers, i, (byte[]) keys[i], stream)) {
                return false;
            }
        }
        return true;
    }
*/
}
