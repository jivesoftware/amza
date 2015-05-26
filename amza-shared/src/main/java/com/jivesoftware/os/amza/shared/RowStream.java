package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public interface RowStream {

    boolean row(long rowFP, long rowTxId, RowType rowType, byte[] row) throws Exception;

}
