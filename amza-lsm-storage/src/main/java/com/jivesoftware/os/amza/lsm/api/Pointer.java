package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public class Pointer {

    public final long timestamp;
    public final boolean tombstone;
    public final long version;
    public final long walFp;

    public Pointer(long timestamp, boolean tombstone, long version, long walFp) {
        this.timestamp = timestamp;
        this.tombstone = tombstone;
        this.version = version;
        this.walFp = walFp;
    }

}
