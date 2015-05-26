package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.WALHighwater;

/**
 *
 * @author jonathan.colt
 */
public interface HighwaterRowMarshaller<R> {

    R toBytes(WALHighwater highwater) throws Exception;

    WALHighwater fromBytes(R row) throws Exception;
}
