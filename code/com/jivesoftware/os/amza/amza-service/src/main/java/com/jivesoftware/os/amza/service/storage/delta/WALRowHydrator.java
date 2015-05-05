package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.storage.WALRow;

/**
 *
 */
public interface WALRowHydrator {

    WALRow hydrate(long fp) throws Exception;
}
