package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeyStream;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;

/**
 *
 * @author jonathan.colt
 */
public class ImmutableWALPointerBlock {

    // TODO consider prefix compression
    private final Object[] keys;
    private final long[] timestamps;
    private final boolean[] tombstoneds;
    private final long[] fps;

    public ImmutableWALPointerBlock(Object[] keys, long[] timestamps, boolean[] tombstoneds, long[] fps) {
        this.keys = keys;
        this.timestamps = timestamps;
        this.tombstoneds = tombstoneds;
        this.fps = fps;
    }

    private static int binarySearch(Object[] a, int fromIndex, int toIndex, byte[] key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            byte[] midVal = (byte[]) a[mid];
            int cmp = UnsignedBytes.lexicographicalComparator().compare(midVal, key);
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

    public boolean getPointer(byte[] key, WALKeyPointerStream stream) throws Exception {
        int i = binarySearch(keys, 0, keys.length, key);
        if (i >= 0) {
            return stream.stream(key, timestamps[i], tombstoneds[i], fps[i]);
        }
        return true;
    }

    // expects walKeys to be in lex order.
    public boolean getPointers(WALKeys walKeys, WALKeyPointerStream stream) throws Exception {
        return walKeys.consume(new WALKeyStream() {
            int lastI = 0;

            @Override
            public boolean stream(byte[] key) throws Exception {
                if (lastI >= keys.length) {
                    return stream.stream(key, -1, false, -1);
                }
                int i = binarySearch(keys, lastI, keys.length, key);
                if (i >= 0) {
                    if (!stream.stream(key, timestamps[i], tombstoneds[i], fps[i])) {
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
        return keyValues.consume(new KeyValueStream() {
            int lastI = 0;

            @Override
            public boolean stream(byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception {
                if (lastI >= keys.length) {
                    return stream.stream(key, value, valueTimestamp, valueTombstoned, -1, false, -1);
                }
                int i = binarySearch(keys, lastI, keys.length, key);
                if (i >= 0) {
                    if (!stream.stream(key, value, valueTimestamp, valueTombstoned, timestamps[i], tombstoneds[i], fps[i])) {
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
        return walKeys.consume(new WALKeyStream() {
            int lastI = 0;

            @Override
            public boolean stream(byte[] key) throws Exception {
                if (lastI >= keys.length) {
                    return stream.stream(key, false);
                }
                int i = binarySearch(keys, lastI, keys.length, key);
                if (i >= 0) {
                    if (!stream.stream(key, tombstoneds[i])) {
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
        return keys.length == 0;
    }

    public long size() throws Exception {
        return keys.length;
    }

    public boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
        int f = binarySearch(keys, 0, keys.length, from);
        if (Math.abs(f + 1) >= keys.length) {
            return true;
        }
        int t = binarySearch(keys, f, keys.length, from);
        for (int i = f; i < t; i++) {
            if (!stream.stream((byte[]) keys[i], timestamps[i], tombstoneds[i], fps[i])) {
                return false;
            }
        }
        return true;
    }

    public boolean rowScan(WALKeyPointerStream stream) throws Exception {
        for (int i = 0; i < keys.length; i++) {
            if (!stream.stream((byte[]) keys[i], timestamps[i], tombstoneds[i], fps[i])) {
                return false;
            }
        }
        return true;
    }

}
