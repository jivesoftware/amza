package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;

/**
 *
 * @author jonathan.colt
 */
class Pointer implements Comparable<Pointer> {
    final int sortIndex;
    final int fi;
    final byte[] key;
    final long timestamps;
    final boolean tombstones;
    final long fps;

    public Pointer(int sortIndex, int fi, byte[] key, long timestamps, boolean tombstones, long fps) {
        this.sortIndex = sortIndex;
        this.fi = fi;
        this.key = key;
        this.timestamps = timestamps;
        this.tombstones = tombstones;
        this.fps = fps;
    }

    @Override
    public int compareTo(Pointer o) {
        int i = UnsignedBytes.lexicographicalComparator().compare(key, o.key);
        if (i != 0) {
            return i;
        }
        return -Long.compare(fps, o.fps);
    }

}
