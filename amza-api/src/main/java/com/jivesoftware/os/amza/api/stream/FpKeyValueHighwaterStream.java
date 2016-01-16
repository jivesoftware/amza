package com.jivesoftware.os.amza.api.stream;

import com.jivesoftware.os.amza.api.wal.WALHighwater;

/**
 *
 * @author jonathan.colt
 */
public interface FpKeyValueHighwaterStream {

    boolean stream(long fp,
        RowType rowType,
        byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstone,
        long valueVersion,
        WALHighwater highwater) throws Exception;

}
