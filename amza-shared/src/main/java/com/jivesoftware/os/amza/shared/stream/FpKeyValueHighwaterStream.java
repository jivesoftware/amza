package com.jivesoftware.os.amza.shared.stream;

import com.jivesoftware.os.amza.shared.wal.WALHighwater;

/**
 *
 * @author jonathan.colt
 */
public interface FpKeyValueHighwaterStream {

    boolean stream(long fp,
        byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstone,
        WALHighwater highwater) throws Exception;

}
