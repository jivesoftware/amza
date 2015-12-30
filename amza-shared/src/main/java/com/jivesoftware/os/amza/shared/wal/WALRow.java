package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.api.stream.RowType;
import java.util.Arrays;

/**
 *
 */
public class WALRow {

    public final RowType rowType;
    public final byte[] prefix;
    public final byte[] key;
    public final byte[] value;
    public final long timestamp;
    public final boolean tombstoned;
    public final long version;

    public WALRow(RowType rowType, byte[] prefix, byte[] key, byte[] value, long timestamp, boolean tombstoned, long version) {
        this.rowType = rowType;
        this.prefix = prefix;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.tombstoned = tombstoned;
        this.version = version;
    }

    @Override
    public String toString() {
        return "WALRow{"
            + "rowType=" + rowType
            + ", prefix=" + Arrays.toString(prefix)
            + ", key=" + Arrays.toString(key)
            + ", value=" + Arrays.toString(value)
            + ", timestamp=" + timestamp
            + ", tombstoned=" + tombstoned
            + ", version=" + version
            + '}';
    }
}
