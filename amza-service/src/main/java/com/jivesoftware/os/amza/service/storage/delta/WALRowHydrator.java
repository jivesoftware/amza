package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.shared.stream.Fps;
import com.jivesoftware.os.amza.shared.wal.WALValue;

/**
 *
 */
public interface WALRowHydrator {

    boolean hydrate(Fps fps, FpKeyValueStream fpKeyValueStream) throws Exception;

    WALValue hydrate(long fp) throws Exception;
}
