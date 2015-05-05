package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public interface RowStream {

    boolean row(long rowFP, long rowTxId, byte rowType, byte[] row) throws Exception;

}
