package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.api.IoStats;
import com.jivesoftware.os.amza.api.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.api.stream.Fps;
import com.jivesoftware.os.amza.api.wal.WALValue;

/**
 *
 */
public interface WALRowHydrator {

    boolean hydrate(IoStats ioStats, Fps fps, FpKeyValueStream fpKeyValueStream) throws Exception;

    WALValue hydrate(long fp) throws Exception;

    void closeHydrator();
}
