package com.jivesoftware.os.amza.storage.binary;

public class BinaryRow {

    public final long transaction;
    public final byte[] key;
    public final long timestamp;
    public final boolean tombstone;
    public final byte[] value;

    public BinaryRow(long transaction, byte[] key, long timestamp, boolean tombstone, byte[] value) {
        this.transaction = transaction;
        this.key = key;
        this.timestamp = timestamp;
        this.tombstone = tombstone;
        this.value = value;
    }

}
