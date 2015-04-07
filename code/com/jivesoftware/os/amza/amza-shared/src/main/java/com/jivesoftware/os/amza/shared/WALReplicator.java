package com.jivesoftware.os.amza.shared;

import java.util.concurrent.Future;

/**
 *
 * @author jonathan.colt
 */
public interface WALReplicator {

    Future<Boolean> replicate(RowsChanged rowsChanged) throws Exception;

}
