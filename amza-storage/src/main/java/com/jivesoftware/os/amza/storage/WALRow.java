package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;

/**
 *
 */
public class WALRow {

    public final WALKey key;
    public final WALValue value;

    public WALRow(WALKey key, WALValue value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "WALRow{" +
            "key=" + key +
            ", value=" + value +
            '}';
    }
}
