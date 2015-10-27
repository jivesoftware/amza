package com.jivesoftware.os.amza.api.take;

import com.jivesoftware.os.amza.api.wal.WALHighwater;

/**
 *
 * @author jonathan.colt
 */
public interface Highwaters {

    void highwater(WALHighwater highwater) throws Exception;
}
