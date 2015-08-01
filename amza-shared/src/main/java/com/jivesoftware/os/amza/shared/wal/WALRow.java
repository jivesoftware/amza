package com.jivesoftware.os.amza.shared.wal;

import java.util.Arrays;

/**
 *
 */
public class WALRow {

    public final byte[] prefix;
    public final byte[] key;
    public final byte[] value;
    public final long timestamp;
    public final boolean tombstoned;

    public WALRow(byte[] prefix, byte[] key, byte[] value, long timestamp, boolean tombstoned) {
        this.prefix = prefix;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.tombstoned = tombstoned;
    }

    @Override
    public String toString() {
        return "WALRow{" +
            "prefix=" + Arrays.toString(prefix) +
            ", key=" + Arrays.toString(key) +
            ", value=" + Arrays.toString(value) +
            ", timestamp=" + timestamp +
            ", tombstoned=" + tombstoned +
            '}';
    }
}
