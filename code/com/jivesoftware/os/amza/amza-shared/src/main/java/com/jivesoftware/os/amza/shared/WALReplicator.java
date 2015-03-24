package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public interface WALReplicator {

    void replicate(RowsChanged rowsChanged) throws Exception;

}
