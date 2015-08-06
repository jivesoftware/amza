package com.jivesoftware.os.amza.lsm;

/**
 *
 * @author jonathan.colt
 */
public class Pointer {

    final int sortIndex;
    final long timestamp;
    final boolean tombstone;
    final long walFp;

    public Pointer(int sortIndex, long timestamp, boolean tombstone, long walFp) {
        this.sortIndex = sortIndex;
        this.timestamp = timestamp;
        this.tombstone = tombstone;
        this.walFp = walFp;
    }

}
