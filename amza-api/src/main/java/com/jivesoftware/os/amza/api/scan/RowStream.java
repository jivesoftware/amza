package com.jivesoftware.os.amza.api.scan;

import com.jivesoftware.os.amza.api.stream.RowType;

/**
 *
 * @author jonathan.colt
 */
public interface RowStream {

    boolean row(long rowFP, long rowTxId, RowType rowType, byte[] row) throws Exception;

}
