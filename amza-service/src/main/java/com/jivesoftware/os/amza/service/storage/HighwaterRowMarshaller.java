package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.api.wal.WALHighwater;

/**
 *
 * @author jonathan.colt
 */
public interface HighwaterRowMarshaller<R> {

    R toBytes(WALHighwater highwater) throws Exception;

    int sizeInBytes(WALHighwater hints);

    WALHighwater fromBytes(R row) throws Exception;
}
