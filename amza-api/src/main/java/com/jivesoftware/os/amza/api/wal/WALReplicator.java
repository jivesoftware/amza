package com.jivesoftware.os.amza.api.wal;

import com.jivesoftware.os.amza.api.scan.RowsChanged;
import java.util.concurrent.Future;

/**
 *
 * @author jonathan.colt
 */
public interface WALReplicator {

    Future<Boolean> replicate(RowsChanged rowsChanged) throws Exception;

}
