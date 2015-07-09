package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.wal.FpKeyValueHighwaterStream;

/**
 *
 */
public interface WALRowHydrator {

    boolean hydrate(long fp, FpKeyValueHighwaterStream stream) throws Exception;
}
