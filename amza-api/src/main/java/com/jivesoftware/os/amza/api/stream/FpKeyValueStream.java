package com.jivesoftware.os.amza.api.stream;

import com.jivesoftware.os.amza.api.stream.RowType;

/**
 *
 * @author jonathan.colt
 */
public interface FpKeyValueStream {

    boolean stream(long fp,
        RowType rowType,
        byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstoned,
        long valueVersion) throws Exception;

}
