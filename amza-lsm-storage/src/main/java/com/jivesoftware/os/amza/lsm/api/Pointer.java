package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public class Pointer {

    public final int sortIndex;
    public final long timestamp;
    public final boolean tombstone;
    public final long walFp;

    public Pointer(int sortIndex, long timestamp, boolean tombstone, long walFp) {
        this.sortIndex = sortIndex;
        this.timestamp = timestamp;
        this.tombstone = tombstone;
        this.walFp = walFp;
    }

}
