package com.jivesoftware.os.amza.storage.binary;

import java.io.Serializable;
import java.util.Arrays;

public class BinaryRow implements Serializable {

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

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 79 * hash + (int) (this.transaction ^ (this.transaction >>> 32));
        hash = 79 * hash + Arrays.hashCode(this.key);
        hash = 79 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
        hash = 79 * hash + (this.tombstone ? 1 : 0);
        hash = 79 * hash + Arrays.hashCode(this.value);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BinaryRow other = (BinaryRow) obj;
        if (this.transaction != other.transaction) {
            return false;
        }
        if (!Arrays.equals(this.key, other.key)) {
            return false;
        }
        if (this.timestamp != other.timestamp) {
            return false;
        }
        if (this.tombstone != other.tombstone) {
            return false;
        }
        if (!Arrays.equals(this.value, other.value)) {
            return false;
        }
        return true;
    }

}
