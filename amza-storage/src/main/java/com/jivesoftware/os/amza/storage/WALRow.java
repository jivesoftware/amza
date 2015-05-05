package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;

/**
 *
 */
public interface WALRow {

    WALKey getKey();

    WALValue getValue();
}
