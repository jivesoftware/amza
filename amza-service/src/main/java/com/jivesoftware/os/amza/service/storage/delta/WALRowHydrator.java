package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.wal.WALRow;

/**
 *
 */
public interface WALRowHydrator {

    WALRow hydrate(long fp) throws Exception;
}
