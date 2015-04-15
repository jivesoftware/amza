package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALValue;

/**
 *
 */
public interface WALValueHydrator {
    WALValue hydrate(WALPointer rowPointer) throws Exception;
}
