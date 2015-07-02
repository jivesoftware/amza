package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.shared.wal.WALHighwater;

/**
 *
 * @author jonathan.colt
 */
public interface HighwaterRowMarshaller<R> {

    R toBytes(WALHighwater highwater) throws Exception;

    WALHighwater fromBytes(R row) throws Exception;
}
