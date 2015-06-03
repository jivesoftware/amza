package com.jivesoftware.os.amza.shared.take;

import com.jivesoftware.os.amza.shared.wal.WALHighwater;

/**
 *
 * @author jonathan.colt
 */
public interface Highwaters {

    void highwater(WALHighwater highwater) throws Exception;
}
