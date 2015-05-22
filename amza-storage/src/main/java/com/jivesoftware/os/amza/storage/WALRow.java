package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;

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

}
