package com.jivesoftware.os.amza.shared.wal;

import java.util.Arrays;

/**
 *
 */
public class WALRow {

    public final byte[] key;
    public final byte[] value;
    public final long timestamp;
    public final boolean tombstoned;

    public WALRow(byte[] key, byte[] value, long timestamp, boolean tombstoned) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.tombstoned = tombstoned;
    }

    public WALKey walKey() {
        return new WALKey(key);
    }

    public WALValue walValue() {
        return new WALValue(value, timestamp, tombstoned);
    }

    @Override
    public String toString() {
        return "WALRow{"
            + "key=" + Arrays.toString(key)
            + ", value=" + Arrays.toString(value)
            + ", timestamp=" + timestamp
            + ", tombstoned=" + tombstoned
            + '}';
    }
}
