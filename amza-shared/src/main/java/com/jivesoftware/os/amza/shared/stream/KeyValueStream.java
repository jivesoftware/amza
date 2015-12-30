package com.jivesoftware.os.amza.shared.stream;

import com.jivesoftware.os.amza.api.stream.RowType;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValueStream {

    boolean stream(RowType rowType,
        byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstoned,
        long valueVersion) throws Exception;

}
