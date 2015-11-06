package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;

/**
 *
 * @author jonathan.colt
 */
class Pointer implements Comparable<Pointer> {

    final byte[] key;
    final long timestamps;
    final long version;
    final boolean tombstones;
    final long fps;

    public Pointer(byte[] key, long timestamps, boolean tombstones, long version, long fps) {
        this.key = key;
        this.timestamps = timestamps;
        this.tombstones = tombstones;
        this.version = version;
        this.fps = fps;
    }

    @Override
    public int compareTo(Pointer o) {
        int i = UnsignedBytes.lexicographicalComparator().compare(key, o.key);
//        if (i != 0) {
//            return i;
//        }
//        return -Long.compare(fps, o.fps);
        return i;
    }

}
